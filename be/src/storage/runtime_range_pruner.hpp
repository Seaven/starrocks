// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstddef>
#include <memory>
#include <utility>

#include "exec/olap_common.h"
#include "exprs/agg_in_runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "runtime/global_dict/config.h"
#include "runtime/runtime_state.h"
#include "storage/column_and_predicate.h"
#include "storage/column_or_predicate.h"
#include "storage/column_predicate.h"
#include "storage/predicate_parser.h"
#include "storage/runtime_range_pruner.h"

namespace starrocks {
namespace detail {
struct RuntimeColumnPredicateBuilder {
    template <LogicalType ltype>
    StatusOr<std::vector<const ColumnPredicate*>> operator()(const ColumnIdToGlobalDictMap* global_dictmaps,
                                                             PredicateParser* parser,
                                                             const RuntimeFilterProbeDescriptor* desc,
                                                             const SlotDescriptor* slot, int32_t driver_sequence,
                                                             ObjectPool* pool) {
        // keep consistent with ColumnRangeBuilder
        if constexpr (ltype == TYPE_TIME || ltype == TYPE_NULL || ltype == TYPE_JSON || lt_is_float<ltype> ||
                      lt_is_binary<ltype>) {
            DCHECK(false) << "unreachable path";
            return Status::NotSupported("unreachable path");
        } else {
            std::vector<const ColumnPredicate*> preds;

            // Treat tinyint and boolean as int
            constexpr LogicalType limit_type = ltype == TYPE_TINYINT || ltype == TYPE_BOOLEAN ? TYPE_INT : ltype;
            // Map TYPE_CHAR to TYPE_VARCHAR
            constexpr LogicalType mapping_type = ltype == TYPE_CHAR ? TYPE_VARCHAR : ltype;

            using value_type = typename RunTimeTypeLimits<limit_type>::value_type;
            using RangeType = ColumnValueRange<value_type>;

            const std::string& col_name = slot->col_name();
            RangeType full_range(col_name, ltype, RunTimeTypeLimits<ltype>::min_value(),
                                 RunTimeTypeLimits<ltype>::max_value());
            if constexpr (lt_is_decimal<limit_type>) {
                full_range.set_precision(slot->type().precision);
                full_range.set_scale(slot->type().scale);
            }

            RangeType& range = full_range;
            range.set_index_filter_only(true);

            const RuntimeFilter* rf = desc->runtime_filter(driver_sequence);

            // process agg in runtime-filter
            auto* in_filter = rf->get_in_filter();
            if (in_filter) {
                if constexpr (ltype == TYPE_VARCHAR) {
                    auto cid = parser->column_id(*slot);
                    if (global_dictmaps == nullptr || global_dictmaps->find(cid) == global_dictmaps->end()) {
                        build_in_range<RangeType, limit_type, mapping_type>(range, rf, pool);
                    }
                } else {
                    build_in_range<RangeType, limit_type, mapping_type>(range, rf, pool);
                }
            }

            // applied global-dict optimized column
            auto* minmax = rf->get_min_max_filter();
            if (minmax) {
                if constexpr (ltype == TYPE_VARCHAR) {
                    auto cid = parser->column_id(*slot);
                    if (auto iter = global_dictmaps->find(cid); iter != global_dictmaps->end()) {
                        build_minmax_range<RangeType, limit_type, LowCardDictType, GlobalDictCodeDecoder>(
                                range, minmax, pool, iter->second);
                    } else {
                        build_minmax_range<RangeType, limit_type, mapping_type, DummyDecoder>(range, minmax, pool,
                                                                                              nullptr);
                    }
                } else {
                    build_minmax_range<RangeType, limit_type, mapping_type, DummyDecoder>(range, minmax, pool, nullptr);
                }
            }

            std::vector<OlapCondition> filters;
            range.to_olap_filter(filters);

            // if runtime filter generate an empty range we could return directly
            if (range.is_empty_value_range()) {
                if (rf->has_null()) {
                    std::vector<const ColumnPredicate*> new_preds;
                    TypeInfoPtr type = get_type_info(limit_type, slot->type().precision, slot->type().scale);
                    auto column_id = parser->column_id(*slot);
                    ColumnPredicate* null_pred = pool->add(new_column_null_predicate(type, column_id, true));
                    new_preds.emplace_back(null_pred);
                    return new_preds;
                } else {
                    return Status::EndOfFile("EOF, Filter by always false runtime filter");
                }
            }

            for (auto& f : filters) {
                ColumnPredicate* p = pool->add(parser->parse_thrift_cond(f));
                VLOG(2) << "build runtime predicate:" << p->debug_string();
                p->set_index_filter_only(f.is_index_filter_only);
                preds.emplace_back(p);
            }

            if (rf->has_null() && !preds.empty()) {
                std::vector<const ColumnPredicate*> new_preds;
                auto type = preds[0]->type_info_ptr();
                auto column_id = preds[0]->column_id();

                ColumnAndPredicate* and_pred = pool->add(new ColumnAndPredicate(type, column_id));
                and_pred->add_child(preds.begin(), preds.end());

                ColumnPredicate* null_pred = pool->add(new_column_null_predicate(type, column_id, true));

                ColumnOrPredicate* or_pred = pool->add(new ColumnOrPredicate(type, column_id));
                or_pred->add_child(and_pred);
                or_pred->add_child(null_pred);
                new_preds.emplace_back(or_pred);

                return new_preds;
            } else {
                return preds;
            }
        }
    }

    template <class InputType>
    struct DummyDecoder {
        DummyDecoder(std::nullptr_t) {}
        auto decode(InputType input) const { return input; }
    };

    template <class InputType>
    struct GlobalDictCodeDecoder {
        GlobalDictCodeDecoder(const GlobalDictMap* dict_map) : _dict_map(dict_map) {}
        Slice decode(DictId input) const {
            for (const auto& [k, v] : *_dict_map) {
                if (v == input) {
                    return k;
                }
            }
            if (input < 0) {
                return Slice::min_value();
            } else {
                return Slice::max_value();
            }
        }

    private:
        const GlobalDictMap* _dict_map;
    };

    template <class RuntimeFilter, class Decoder>
    struct MinMaxParser {
        MinMaxParser(const RuntimeFilter* runtime_filter_, Decoder* decoder)
                : runtime_filter(runtime_filter_), decoder(decoder) {}
        auto min_value(ObjectPool* pool) {
            auto code = runtime_filter->min_value(pool);
            return decoder->decode(code);
        }
        auto max_value(ObjectPool* pool) {
            auto code = runtime_filter->max_value(pool);
            return decoder->decode(code);
        }

        template <LogicalType Type>
        ColumnPtr min_const_column(const TypeDescriptor& col_type, ObjectPool* pool) {
            auto min_decode_value = min_value(pool);
            if constexpr (lt_is_decimal<Type>) {
                return ColumnHelper::create_const_decimal_column<Type>(min_decode_value, col_type.precision,
                                                                       col_type.scale, 1);
            } else {
                return ColumnHelper::create_const_column<Type>(min_decode_value, 1);
            }
        }

        template <LogicalType Type>
        ColumnPtr max_const_column(const TypeDescriptor& col_type, ObjectPool* pool) {
            auto max_decode_value = max_value(pool);
            if constexpr (lt_is_decimal<Type>) {
                return ColumnHelper::create_const_decimal_column<Type>(max_decode_value, col_type.precision,
                                                                       col_type.scale, 1);
            } else {
                return ColumnHelper::create_const_column<Type>(max_decode_value, 1);
            }
        }

    private:
        const RuntimeFilter* runtime_filter;
        const Decoder* decoder;
    };

    template <class Range, LogicalType SlotType, LogicalType mapping_type>
    static void build_in_range(Range& range, const RuntimeFilter* rf, ObjectPool* pool) {
        auto* filter = down_cast<const InRuntimeFilter<mapping_type>*>(rf->get_in_filter());
        if (filter == nullptr) return;
        auto hash_set = filter->get_set(pool);
        boost::container::flat_set<typename Range::RangeValueType> values(hash_set.begin(), hash_set.end());
        (void)range.add_fixed_values(FILTER_IN, values);
    }

    template <class Range, LogicalType SlotType, LogicalType mapping_type, template <class> class Decoder,
              class... Args>
    static void build_minmax_range(Range& range, const RuntimeFilter* rf, ObjectPool* pool, Args&&... args) {
        using ValueType = typename RunTimeTypeTraits<SlotType>::CppType;

        auto* filter = down_cast<const MinMaxRuntimeFilter<mapping_type>*>(rf->get_min_max_filter());
        if (filter == nullptr) return;
        using DecoderType = Decoder<typename RunTimeTypeTraits<mapping_type>::CppType>;
        DecoderType decoder(std::forward<Args>(args)...);
        MinMaxParser<MinMaxRuntimeFilter<mapping_type>, DecoderType> parser(filter, &decoder);
        if (filter->is_empty_range()) {
            range.clear_to_empty();
            return;
        }

        SQLFilterOp min_op;
        if (filter->left_close_interval()) {
            min_op = to_olap_filter_type(TExprOpcode::GE, false);
        } else {
            min_op = to_olap_filter_type(TExprOpcode::GT, false);
        }
        auto min_value = parser.min_value(pool);
        (void)range.add_range(min_op, static_cast<ValueType>(min_value));

        SQLFilterOp max_op;
        if (filter->right_close_interval()) {
            max_op = to_olap_filter_type(TExprOpcode::LE, false);
        } else {
            max_op = to_olap_filter_type(TExprOpcode::LT, false);
        }

        auto max_value = parser.max_value(pool);
        (void)range.add_range(max_op, static_cast<ValueType>(max_value));
    }
};
} // namespace detail

inline Status RuntimeScanRangePruner::_update(const ColumnIdToGlobalDictMap* global_dictmaps,
                                              RuntimeFilterArrivedCallBack&& updater, bool force,
                                              size_t raw_read_rows) {
    if (_arrived_runtime_filters_masks.empty()) {
        return Status::OK();
    }
    for (size_t i = 0; i < _arrived_runtime_filters_masks.size(); ++i) {
        // 1. runtime filter arrived
        // 2. runtime filter updated and read rows greater than rf_update_threhold
        // we will filter by index
        if (auto rf = _unarrived_runtime_filters[i]->runtime_filter(_driver_sequence)) {
            size_t rf_version = rf->rf_version();
            if (!_arrived_runtime_filters_masks[i] ||
                (rf_version > _rf_versions[i] && (force || raw_read_rows - _raw_read_rows > rf_update_threshold))) {
                ObjectPool pool;

                ASSIGN_OR_RETURN(auto predicates, _get_predicates(global_dictmaps, i, &pool));
                if (!predicates.empty()) {
                    RETURN_IF_ERROR(updater(predicates.front()->column_id(), predicates));
                }
                _arrived_runtime_filters_masks[i] = true;
                _rf_versions[i] = rf_version;
                _raw_read_rows = raw_read_rows;
            }
        }
    }

    return Status::OK();
}

inline auto RuntimeScanRangePruner::_get_predicates(const ColumnIdToGlobalDictMap* global_dictmaps, size_t idx,
                                                    ObjectPool* pool) -> StatusOr<PredicatesRawPtrs> {
    // convert to olap filter
    auto slot_desc = _slot_descs[idx];
    return type_dispatch_predicate<StatusOr<PredicatesRawPtrs>>(
            slot_desc->type().type, false, detail::RuntimeColumnPredicateBuilder(), global_dictmaps, _parser,
            _unarrived_runtime_filters[idx], slot_desc, _driver_sequence, pool);
}

inline void RuntimeScanRangePruner::_init(const UnarrivedRuntimeFilterList& params) {
    for (size_t i = 0; i < params.slot_descs.size(); ++i) {
        if (_parser->can_pushdown(params.slot_descs[i])) {
            _unarrived_runtime_filters.emplace_back(params.unarrived_runtime_filters[i]);
            _slot_descs.emplace_back(params.slot_descs[i]);
            _arrived_runtime_filters_masks.emplace_back();
            _rf_versions.emplace_back();
            _driver_sequence = params.driver_sequence;
        }
    }
}

} // namespace starrocks
