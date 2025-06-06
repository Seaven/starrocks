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

#include "exprs/runtime_filter_bank.h"

#include <runtime/runtime_filter_worker.h>
#include <serde/column_array_serde.h>

#include <memory>
#include <thread>

#include "column/column.h"
#include "exec/pipeline/runtime_filter_types.h"
#include "exprs/agg_in_runtime_filter.h"
#include "exprs/dictmapping_expr.h"
#include "exprs/in_const_predicate.hpp"
#include "exprs/literal.h"
#include "exprs/min_max_predicate.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_layout.h"
#include "gen_cpp/RuntimeFilter_types.h"
#include "gen_cpp/Types_types.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_filter_cache.h"
#include "runtime/runtime_state.h"
#include "simd/simd.h"
#include "types/logical_type.h"
#include "types/logical_type_infra.h"
#include "util/time.h"

namespace starrocks {
RuntimeFilter* RuntimeFilterHelper::transmit_to_runtime_empty_filter(ObjectPool* pool, RuntimeFilter* rf) {
    const auto* min_max_filter = rf->get_min_max_filter();
    const auto* membership_filter = rf->get_membership_filter();
    RuntimeFilter* filter = type_dispatch_filter(
            membership_filter->logical_type(), static_cast<RuntimeFilter*>(nullptr),
            [&]<LogicalType LT>() -> RuntimeFilter* {
                return new ComposedRuntimeEmptyFilter<LT>(*down_cast<const MinMaxRuntimeFilter<LT>*>(min_max_filter),
                                                          *membership_filter);
            });

    if (pool != nullptr && filter != nullptr) {
        return pool->add(filter);
    } else {
        return filter;
    }
}

template <template <LogicalType> typename FilterType>
static RuntimeFilter* create_runtime_filter_helper(ObjectPool* pool, LogicalType type, int8_t join_mode) {
    RuntimeFilter* filter = type_dispatch_filter(type, static_cast<RuntimeFilter*>(nullptr), [&]<LogicalType LT>() {
        RuntimeFilter* rf = new FilterType<LT>();
        rf->get_membership_filter()->set_join_mode(join_mode);
        return rf;
    });

    if (pool != nullptr && filter != nullptr) {
        return pool->add(filter);
    } else {
        return filter;
    }
}

RuntimeFilter* RuntimeFilterHelper::create_runtime_empty_filter(ObjectPool* pool, LogicalType type, int8_t join_mode) {
    return create_runtime_filter_helper<ComposedRuntimeEmptyFilter>(pool, type, join_mode);
}

RuntimeFilter* RuntimeFilterHelper::create_runtime_bloom_filter(ObjectPool* pool, LogicalType type, int8_t join_mode) {
    return create_runtime_filter_helper<ComposedRuntimeBloomFilter>(pool, type, join_mode);
}

RuntimeFilter* RuntimeFilterHelper::create_agg_runtime_in_filter(ObjectPool* pool, LogicalType type, int8_t join_mode) {
    return scalar_type_dispatch(type, [pool]<LogicalType ltype>() -> RuntimeFilter* {
        auto rf = new InRuntimeFilter<ltype>();
        if (pool != nullptr) {
            return pool->add(rf);
        }
        return rf;
    });
}

RuntimeFilter* RuntimeFilterHelper::create_runtime_bitset_filter(ObjectPool* pool, LogicalType type, int8_t join_mode) {
    RuntimeFilter* filter =
            type_dispatch_bitset_filter(type, static_cast<RuntimeFilter*>(nullptr), [&]<LogicalType LT>() {
                RuntimeFilter* rf = new ComposedRuntimeBitsetFilter<LT>();
                rf->get_membership_filter()->set_join_mode(join_mode);
                return rf;
            });

    if (pool != nullptr && filter != nullptr) {
        return pool->add(filter);
    } else {
        return filter;
    }
}

RuntimeFilter* RuntimeFilterHelper::create_runtime_filter(ObjectPool* pool, RuntimeFilterSerializeType rf_type,
                                                          LogicalType ltype, int8_t join_mode) {
    switch (rf_type) {
    case RuntimeFilterSerializeType::EMPTY_FILTER:
        return create_runtime_empty_filter(pool, ltype, join_mode);
    case RuntimeFilterSerializeType::BLOOM_FILTER:
        return create_runtime_bloom_filter(pool, ltype, join_mode);
    case RuntimeFilterSerializeType::BITSET_FILTER:
        return create_runtime_bitset_filter(pool, ltype, join_mode);
    case RuntimeFilterSerializeType::IN_FILTER:
        return create_agg_runtime_in_filter(pool, ltype, join_mode);
    case RuntimeFilterSerializeType::NONE:
    default:
        return nullptr;
    }
}

template <LogicalType LT, typename CppType = RunTimeCppType<LT>>
static std::pair<CppType, CppType> calc_min_max_from_columns(const Columns& columns, size_t column_offset) {
    auto min_value = std::numeric_limits<CppType>::max();
    auto max_value = std::numeric_limits<CppType>::min();
    for (const auto& col : columns) {
        if (!col->is_nullable()) {
            const auto& values = GetContainer<LT>::get_data(col.get());
            for (size_t i = column_offset; i < values.size(); i++) {
                min_value = std::min(min_value, values[i]);
                max_value = std::max(max_value, values[i]);
            }
        } else {
            const auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(col);
            const auto& values = GetContainer<LT>::get_data(nullable_column->data_column().get());
            if (!nullable_column->has_null()) {
                for (size_t i = column_offset; i < values.size(); i++) {
                    min_value = std::min(min_value, values[i]);
                    max_value = std::max(max_value, values[i]);
                }
            } else {
                const auto& null_data = nullable_column->immutable_null_column_data();
                for (size_t i = column_offset; i < values.size(); i++) {
                    if (null_data[i] == 0) {
                        min_value = std::min(min_value, values[i]);
                        max_value = std::max(max_value, values[i]);
                    }
                }
            }
        }
    }

    return {min_value, max_value};
}

struct try_create_runtime_bitset_filter {
    template <LogicalType LT>
    RuntimeFilter* operator()(const pipeline::RuntimeMembershipFilterBuildParam& param, size_t column_offset,
                              size_t row_count) {
        static auto cache_sizes = [] {
            static constexpr size_t DEFAULT_L2_CACHE_SIZE = 1 * 1024 * 1024;
            static constexpr size_t DEFAULT_L3_CACHE_SIZE = 32 * 1024 * 1024;

            const auto& cache_sizes = CpuInfo::get_cache_sizes();
            const auto cur_l2_cache_size = cache_sizes[CpuInfo::L2_CACHE];
            const auto cur_l3_cache_size = cache_sizes[CpuInfo::L3_CACHE];
            return std::pair{cur_l2_cache_size ? cur_l2_cache_size : DEFAULT_L2_CACHE_SIZE,
                             cur_l3_cache_size ? cur_l3_cache_size : DEFAULT_L3_CACHE_SIZE};
        }();
        const auto& [l2_cache_size, l3_cache_size] = cache_sizes;

        const auto [min_value, max_value] = calc_min_max_from_columns<LT>(param.columns, column_offset);
        const size_t value_interval = static_cast<size_t>(RuntimeBitsetFilter<LT>::to_numeric(max_value)) -
                                      RuntimeBitsetFilter<LT>::to_numeric(min_value) + 1;

        VLOG_FILE << "[BITSET] min_value=" << min_value << ", max_value=" << max_value
                  << ", value_interval=" << value_interval;

        if (min_value > max_value || value_interval == 0) { // Overflow.
            return nullptr;
        }

        const size_t bitset_memory_usage = (value_interval + 7) / 8;
        const size_t bloom_memory_usage = row_count;
        if (bitset_memory_usage <= bloom_memory_usage ||
            (bitset_memory_usage <= bloom_memory_usage * 4 && bitset_memory_usage <= l2_cache_size) ||
            (bitset_memory_usage <= bloom_memory_usage * 2 && bitset_memory_usage <= l3_cache_size)) {
            auto* rf = new ComposedRuntimeBitsetFilter<LT>();
            rf->membership_filter().set_min_max(min_value, max_value);
            return rf;
        }

        return nullptr;
    }
};

RuntimeFilter* RuntimeFilterHelper::create_join_runtime_filter(ObjectPool* pool, LogicalType type, int8_t join_mode,
                                                               const pipeline::RuntimeMembershipFilterBuildParam& param,
                                                               size_t column_offset, size_t row_count) {
    RuntimeFilter* filter =
            type_dispatch_bitset_filter(type, static_cast<RuntimeFilter*>(nullptr), try_create_runtime_bitset_filter(),
                                        param, column_offset, row_count);
    if (filter == nullptr) { // Fall back to runtime bloom filter.
        return create_runtime_bloom_filter(pool, type, join_mode);
    } else {
        if (pool != nullptr) {
            return pool->add(filter);
        } else {
            return filter;
        }
    }
}

static uint8_t get_rf_version(const RuntimeState* state) {
    if (state->func_version() >= TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_3) {
        return RF_VERSION_V3;
    } else if (state->func_version() >= TFunctionVersion::type::RUNTIME_FILTER_SERIALIZE_VERSION_2) {
        return RF_VERSION_V2;
    }
    return RF_VERSION;
}

size_t RuntimeFilterHelper::max_runtime_filter_serialized_size(int rf_version, const RuntimeFilter* rf) {
    // 1. rf_version
    size_t size = RF_VERSION_SZ;
    // 2. rf_type
    if (rf_version >= RF_VERSION_V3) {
        size += sizeof(RuntimeFilterSerializeType);
    }
    // 3. the specific rf.
    size += rf->max_serialized_size();
    return size;
}

size_t RuntimeFilterHelper::max_runtime_filter_serialized_size(RuntimeState* state, const RuntimeFilter* rf) {
    const uint8_t rf_version = get_rf_version(state);
    return max_runtime_filter_serialized_size(rf_version, rf);
}

size_t RuntimeFilterHelper::serialize_runtime_filter(int rf_version, const RuntimeFilter* rf, uint8_t* data) {
    size_t offset = 0;

    // 1. rf_version
    memcpy(data + offset, &rf_version, RF_VERSION_SZ);
    offset += RF_VERSION_SZ;

    // 2. rf_type
    if (rf_version >= RF_VERSION_V3) {
        const RuntimeFilterSerializeType type = rf->type();
        memcpy(data + offset, &type, sizeof(type));
        offset += sizeof(type);
    }

    // 3. the specific rf.
    offset += rf->serialize(rf_version, data + offset);

    return offset;
}

size_t RuntimeFilterHelper::serialize_runtime_filter(RuntimeState* state, const RuntimeFilter* rf, uint8_t* data) {
    const uint8_t rf_version = get_rf_version(state);
    return serialize_runtime_filter(rf_version, rf, data);
}

int RuntimeFilterHelper::deserialize_runtime_filter(ObjectPool* pool, RuntimeFilter** rf, const uint8_t* data,
                                                    size_t size) {
    *rf = nullptr;

    size_t offset = 0;

    // 1. rf_version
    uint8_t version = 0;
    memcpy(&version, data, sizeof(version));
    offset += sizeof(version);
    if (version < RF_VERSION_V2) {
        // version mismatch and skip this chunk.
        LOG(WARNING) << "unrecognized version:" << version;
        return 0;
    }

    // 2. rf_type
    RuntimeFilterSerializeType rf_type = RuntimeFilterSerializeType::BLOOM_FILTER;
    if (version >= RF_VERSION_V3) {
        memcpy(&rf_type, data + offset, sizeof(rf_type));
        offset += sizeof(rf_type);
    }
    if (rf_type > RuntimeFilterSerializeType::UNKNOWN_FILTER) {
        LOG(WARNING) << "unrecognized runtime filter type:" << static_cast<int>(rf_type);
        return 0;
    }

    // 3. peek logical type.
    TPrimitiveType::type tltype;
    memcpy(&tltype, data + offset, sizeof(tltype));
    LogicalType ltype = thrift_to_type(tltype);

    // 4. deserialize the specific rf.
    RuntimeFilter* filter = create_runtime_filter(pool, rf_type, ltype, TJoinDistributionMode::NONE);
    DCHECK(filter != nullptr);
    if (filter != nullptr) {
        offset += filter->deserialize(version, data + offset);
        DCHECK(offset == size);
        *rf = filter;
    }

    return version;
}

size_t RuntimeFilterHelper::max_runtime_filter_serialized_size_for_skew_boradcast_join(const ColumnPtr& column) {
    size_t size = RF_VERSION_SZ;
    size += (sizeof(bool) + sizeof(size_t) + sizeof(bool) + sizeof(bool));
    size += serde::ColumnArraySerde::max_serialized_size(*column);
    return size;
}

size_t RuntimeFilterHelper::serialize_runtime_filter_for_skew_broadcast_join(const ColumnPtr& column, bool eq_null,
                                                                             uint8_t* data) {
    size_t offset = 0;
#define JRF_COPY_FIELD_TO(field)                  \
    memcpy(data + offset, &field, sizeof(field)); \
    offset += sizeof(field);
    // put version at the head.
    JRF_COPY_FIELD_TO(RF_VERSION_V2);
    JRF_COPY_FIELD_TO(eq_null);
    size_t num_rows = column->size();
    JRF_COPY_FIELD_TO(num_rows);
    bool is_nullable = column->is_nullable();
    JRF_COPY_FIELD_TO(is_nullable)
    bool is_const = column->is_constant();
    JRF_COPY_FIELD_TO(is_const);

    uint8_t* cur = data + offset;
    cur = serde::ColumnArraySerde::serialize(*column, cur);
    offset += (cur - (data + offset));

    return offset;
}

// |version|eq_null|num_rows|is_null|is_const|type|column_data|
int RuntimeFilterHelper::deserialize_runtime_filter_for_skew_broadcast_join(ObjectPool* pool,
                                                                            SkewBroadcastRfMaterial** material,
                                                                            const uint8_t* data, size_t size,
                                                                            const PTypeDesc& ptype) {
    *material = nullptr;
    SkewBroadcastRfMaterial* rf_material = pool->add(new SkewBroadcastRfMaterial());
    size_t offset = 0;

#define JRF_COPY_FIELD_FROM(field)                \
    memcpy(&field, data + offset, sizeof(field)); \
    offset += sizeof(field);

    // read version first.
    uint8_t version = 0;
    JRF_COPY_FIELD_FROM(version);
    if (version != RF_VERSION_V2) {
        LOG(WARNING) << "unrecognized version:" << version;
        return 0;
    }

    // read eq_null
    bool eq_null;
    JRF_COPY_FIELD_FROM(eq_null);

    // read key column [num_rows,is_null, is_const,type,column_data]
    size_t num_rows = 0;
    JRF_COPY_FIELD_FROM(num_rows);

    bool is_null;
    JRF_COPY_FIELD_FROM(is_null);

    bool is_const;
    JRF_COPY_FIELD_FROM(is_const);

    TypeDescriptor type_descriptor = TypeDescriptor::from_protobuf(ptype);

    auto columnPtr = ColumnHelper::create_column(type_descriptor, is_null, is_const, num_rows);

    const uint8_t* cur = data + offset;
    cur = serde::ColumnArraySerde::deserialize(cur, columnPtr.get());
    offset += (cur - (data + offset));

    DCHECK(offset == size);

    rf_material->build_type = type_descriptor.type;
    rf_material->eq_null = eq_null;
    rf_material->key_column = columnPtr;

    *material = rf_material;
    return version;
}

template <template <LogicalType> typename FilterType, bool is_skew_join>
struct FilterIniter {
    template <LogicalType LT>
    Status operator()(const ColumnPtr& column, size_t column_offset, RuntimeFilter* expr, bool eq_null) {
        auto* filter = down_cast<FilterType<LT>*>(expr);

        if (column->is_nullable()) {
            auto* nullable_column = ColumnHelper::as_raw_column<NullableColumn>(column);
            const auto& data_array = GetContainer<LT>::get_data(nullable_column->data_column().get());
            if (!nullable_column->has_null()) {
                for (size_t j = column_offset; j < data_array.size(); j++) {
                    if constexpr (is_skew_join) {
                        filter->insert_skew_values(data_array[j]);
                    } else {
                        filter->insert(data_array[j]);
                    }
                }
            } else {
                for (size_t j = column_offset; j < data_array.size(); j++) {
                    if (!nullable_column->is_null(j)) {
                        filter->insert(data_array[j]);
                    } else {
                        if (eq_null) {
                            filter->insert_null();
                        }
                    }
                }
            }
        } else {
            const auto& data_array = GetContainer<LT>::get_data(column.get());
            for (size_t j = column_offset; j < data_array.size(); j++) {
                if constexpr (is_skew_join) {
                    filter->insert_skew_values(data_array[j]);
                } else {
                    filter->insert(data_array[j]);
                }
            }
        }
        return Status::OK();
    }
};

Status RuntimeFilterHelper::fill_runtime_filter(const ColumnPtr& column, LogicalType type, RuntimeFilter* filter,
                                                size_t column_offset, bool eq_null, bool is_skew_join) {
    if (column->has_large_column()) {
        return Status::NotSupported("unsupported build runtime filter for large binary column");
    }

    const auto rf_type = filter->type();
    switch (rf_type) {
    case RuntimeFilterSerializeType::BLOOM_FILTER:
        if (is_skew_join) {
            return type_dispatch_filter(type, Status::OK(), FilterIniter<ComposedRuntimeBloomFilter, true>(), column,
                                        column_offset, filter, eq_null);
        } else {
            return type_dispatch_filter(type, Status::OK(), FilterIniter<ComposedRuntimeBloomFilter, false>(), column,
                                        column_offset, filter, eq_null);
        }
    case RuntimeFilterSerializeType::BITSET_FILTER: {
        const auto error_status = Status::NotSupported("runtime bitset filter do not support the logical type: " +
                                                       std::string(logical_type_to_string(type)));
        return type_dispatch_bitset_filter(type, error_status, FilterIniter<ComposedRuntimeBitsetFilter, false>(),
                                           column, column_offset, filter, eq_null);
    }
    case RuntimeFilterSerializeType::EMPTY_FILTER:
        return type_dispatch_filter(type, Status::OK(), FilterIniter<ComposedRuntimeEmptyFilter, false>(), column,
                                    column_offset, filter, eq_null);
    case RuntimeFilterSerializeType::NONE:
    default:
        return Status::NotSupported("unsupported build runtime filter: " + filter->debug_string());
    }
}

Status RuntimeFilterHelper::fill_runtime_filter(const Columns& columns, LogicalType type, RuntimeFilter* filter,
                                                size_t column_offset, bool eq_null) {
    for (const auto& column : columns) {
        RETURN_IF_ERROR(fill_runtime_filter(column, type, filter, column_offset, eq_null));
    }
    return Status::OK();
}

Status RuntimeFilterHelper::fill_runtime_filter(const pipeline::RuntimeMembershipFilterBuildParam& param,
                                                LogicalType type, RuntimeFilter* filter, size_t column_offset) {
    return fill_runtime_filter(param.columns, type, filter, column_offset, param.eq_null);
}

StatusOr<ExprContext*> RuntimeFilterHelper::rewrite_runtime_filter_in_cross_join_node(ObjectPool* pool,
                                                                                      ExprContext* conjunct,
                                                                                      Chunk* chunk) {
    auto left_child = conjunct->root()->get_child(0);
    auto right_child = conjunct->root()->get_child(1);
    // all of the child(1) in expr is in build chunk
    ASSIGN_OR_RETURN(auto res, conjunct->evaluate(right_child, chunk));
    DCHECK_EQ(res->size(), 1);
    ColumnPtr col;
    if (res->is_constant()) {
        col = res;
    } else if (res->is_nullable()) {
        if (res->is_null(0)) {
            col = ColumnHelper::create_const_null_column(1);
        } else {
            auto data_col = down_cast<NullableColumn*>(res.get())->data_column();
            col = ConstColumn::create(std::move(data_col), 1);
        }
    } else {
        col = ConstColumn::create(std::move(res), 1);
    }

    auto literal = pool->add(new VectorizedLiteral(std::move(col), right_child->type()));
    auto new_expr = conjunct->root()->clone(pool);
    auto new_left = left_child->clone(pool);
    new_expr->clear_children();
    new_expr->add_child(new_left);
    new_expr->add_child(literal);
    auto expr = pool->add(new ExprContext(new_expr));
    expr->set_build_from_only_in_filter(true);
    return expr;
}

Status RuntimeFilterBuildDescriptor::init(ObjectPool* pool, const TRuntimeFilterDescription& desc,
                                          RuntimeState* state) {
    _filter_id = desc.filter_id;
    _build_expr_order = desc.expr_order;
    _has_remote_targets = desc.has_remote_targets;

    if (desc.__isset.runtime_filter_merge_nodes) {
        _merge_nodes = desc.runtime_filter_merge_nodes;
    }
    _has_consumer = false;
    _join_mode = desc.build_join_mode;
    if (desc.__isset.plan_node_id_to_target_expr && desc.plan_node_id_to_target_expr.size() != 0) {
        _has_consumer = true;
    }
    if (!desc.__isset.build_expr) {
        return Status::NotFound("build_expr not found");
    }
    if (desc.__isset.sender_finst_id) {
        _sender_finst_id = desc.sender_finst_id;
    }
    if (desc.__isset.broadcast_grf_senders) {
        _broadcast_grf_senders.insert(desc.broadcast_grf_senders.begin(), desc.broadcast_grf_senders.end());
    }
    if (desc.__isset.broadcast_grf_destinations) {
        _broadcast_grf_destinations = desc.broadcast_grf_destinations;
    }
    if (desc.__isset.is_broad_cast_join_in_skew) {
        _is_broad_cast_in_skew = desc.is_broad_cast_join_in_skew;
    }
    if (desc.__isset.skew_shuffle_filter_id) {
        _skew_shuffle_filter_id = desc.skew_shuffle_filter_id;
    }

    WithLayoutMixin::init(desc);
    RETURN_IF_ERROR(Expr::create_expr_tree(pool, desc.build_expr, &_build_expr_ctx, state));
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::init(ObjectPool* pool, const TRuntimeFilterDescription& desc, TPlanNodeId node_id,
                                          RuntimeState* state) {
    _filter_id = desc.filter_id;
    _is_local = !desc.has_remote_targets;
    _build_plan_node_id = desc.build_plan_node_id;
    _runtime_filter.store(nullptr);
    _join_mode = desc.build_join_mode;
    _is_stream_build_filter = desc.__isset.filter_type && (desc.filter_type == TRuntimeFilterBuildType::TOPN_FILTER ||
                                                           desc.filter_type == TRuntimeFilterBuildType::AGG_FILTER);
    _skip_wait = _is_stream_build_filter;
    _is_group_colocate_rf = desc.__isset.build_from_group_execution && desc.build_from_group_execution;

    bool not_found = true;
    if (desc.__isset.plan_node_id_to_target_expr) {
        const auto& it = const_cast<TRuntimeFilterDescription&>(desc).plan_node_id_to_target_expr.find(node_id);
        if (it != desc.plan_node_id_to_target_expr.end()) {
            not_found = false;
            RETURN_IF_ERROR(Expr::create_expr_tree(pool, it->second, &_probe_expr_ctx, state));
        }
    }

    WithLayoutMixin::init(desc);

    if (desc.__isset.plan_node_id_to_partition_by_exprs) {
        const auto& it = const_cast<TRuntimeFilterDescription&>(desc).plan_node_id_to_partition_by_exprs.find(node_id);
        // TODO(lishuming): maybe reuse probe exprs because partition_by_exprs and probe_expr
        // must be overlapped.
        if (it != desc.plan_node_id_to_partition_by_exprs.end()) {
            RETURN_IF_ERROR(Expr::create_expr_trees(pool, it->second, &_partition_by_exprs_contexts, state));
        }
    }

    if (not_found) {
        return Status::NotFound("plan node id not found. node_id = " + std::to_string(node_id));
    }
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::init(int32_t filter_id, ExprContext* probe_expr_ctx) {
    _filter_id = filter_id;
    _probe_expr_ctx = probe_expr_ctx;
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::prepare(RuntimeState* state, RuntimeProfile* p) {
    if (_probe_expr_ctx != nullptr) {
        RETURN_IF_ERROR(_probe_expr_ctx->prepare(state));
    }
    for (auto* partition_by_expr : _partition_by_exprs_contexts) {
        RETURN_IF_ERROR(partition_by_expr->prepare(state));
    }
    _open_timestamp = UnixMillis();
    _latency_timer = ADD_COUNTER(p, strings::Substitute("JoinRuntimeFilter/$0/latency", _filter_id), TUnit::TIME_NS);
    // not set yet.
    _latency_timer->set((int64_t)(-1));
    return Status::OK();
}

Status RuntimeFilterProbeDescriptor::open(RuntimeState* state) {
    if (_probe_expr_ctx != nullptr) {
        RETURN_IF_ERROR(_probe_expr_ctx->open(state));
    }
    for (auto* partition_by_expr : _partition_by_exprs_contexts) {
        RETURN_IF_ERROR(partition_by_expr->open(state));
    }
    return Status::OK();
}

void RuntimeFilterProbeDescriptor::close(RuntimeState* state) {
    if (_probe_expr_ctx != nullptr) {
        _probe_expr_ctx->close(state);
    }
    for (auto* partition_by_expr : _partition_by_exprs_contexts) {
        partition_by_expr->close(state);
    }
}

void RuntimeFilterProbeDescriptor::replace_probe_expr_ctx(RuntimeState* state, const RowDescriptor& row_desc,
                                                          ExprContext* new_probe_expr_ctx) {
    // close old probe expr
    _probe_expr_ctx->close(state);
    // create new probe expr and open it.
    _probe_expr_ctx = state->obj_pool()->add(new ExprContext(new_probe_expr_ctx->root()));
    WARN_IF_ERROR(_probe_expr_ctx->prepare(state), "prepare probe expr failed");
    WARN_IF_ERROR(_probe_expr_ctx->open(state), "open probe expr failed");
}

std::string RuntimeFilterProbeDescriptor::debug_string() const {
    std::stringstream ss;
    ss << "RFDptr(filter_id=" << _filter_id << ", probe_expr=";
    if (_probe_expr_ctx != nullptr) {
        ss << "(addr = " << _probe_expr_ctx << ", expr = " << _probe_expr_ctx->root()->debug_string() << ")";
    } else {
        ss << "nullptr";
    }
    ss << ", is_local=" << _is_local;
    ss << ", is_topn=" << _is_stream_build_filter;
    ss << ", rf=";
    const RuntimeFilter* rf = _runtime_filter.load();
    if (rf != nullptr) {
        ss << rf->debug_string();
    } else {
        ss << "nullptr";
    }
    ss << ")";
    return ss.str();
}

static const int default_runtime_filter_wait_timeout_ms = 1000;

RuntimeFilterProbeCollector::RuntimeFilterProbeCollector() : _wait_timeout_ms(default_runtime_filter_wait_timeout_ms) {}

RuntimeFilterProbeCollector::RuntimeFilterProbeCollector(RuntimeFilterProbeCollector&& that) noexcept
        : _descriptors(std::move(that._descriptors)),
          _wait_timeout_ms(that._wait_timeout_ms),
          _scan_wait_timeout_ms(that._scan_wait_timeout_ms),
          _eval_context(that._eval_context),
          _plan_node_id(that._plan_node_id) {}

Status RuntimeFilterProbeCollector::prepare(RuntimeState* state, RuntimeProfile* profile) {
    _runtime_profile = profile;
    _runtime_state = state;
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = it.second;
        RETURN_IF_ERROR(rf_desc->prepare(state, profile));
    }
    if (state != nullptr) {
        const TQueryOptions& options = state->query_options();
        if (options.__isset.runtime_filter_early_return_selectivity) {
            _early_return_selectivity = options.runtime_filter_early_return_selectivity;
        }
    }
    return Status::OK();
}
Status RuntimeFilterProbeCollector::open(RuntimeState* state) {
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = it.second;
        RETURN_IF_ERROR(rf_desc->open(state));
    }
    return Status::OK();
}
void RuntimeFilterProbeCollector::close(RuntimeState* state) {
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = it.second;
        rf_desc->close(state);
    }
}

// do_evaluate is reentrant, can be called concurrently by multiple operators that shared the same
// RuntimeFilterProbeCollector.
void RuntimeFilterProbeCollector::do_evaluate(Chunk* chunk, RuntimeMembershipFilterEvalContext& eval_context) {
    if (eval_context.mode == RuntimeMembershipFilterEvalContext::Mode::M_ONLY_TOPN) {
        update_selectivity(chunk, eval_context);
        return;
    } else {
        if ((eval_context.input_chunk_nums++ & 31) == 0) {
            update_selectivity(chunk, eval_context);
            return;
        }
    }

    auto& seletivity_map = eval_context.selectivity;
    if (seletivity_map.empty()) {
        return;
    }

    auto& selection = eval_context.running_context.selection;
    eval_context.running_context.use_merged_selection = false;
    eval_context.running_context.compatibility =
            _runtime_state->func_version() <= 3 || !_runtime_state->enable_pipeline_engine();

    for (auto& kv : seletivity_map) {
        RuntimeFilterProbeDescriptor* rf_desc = kv.second;
        const RuntimeFilter* filter = rf_desc->runtime_filter(eval_context.driver_sequence);
        bool skip_topn = eval_context.mode == RuntimeMembershipFilterEvalContext::Mode::M_WITHOUT_TOPN;
        if ((skip_topn && rf_desc->is_stream_build_filter()) || filter == nullptr || filter->always_true()) {
            continue;
        }
        if (rf_desc->has_push_down_to_storage()) {
            continue;
        }

        auto* ctx = rf_desc->probe_expr_ctx();
        ColumnPtr column = EVALUATE_NULL_IF_ERROR(ctx, ctx->root(), chunk);

        // for colocate grf
        compute_hash_values(chunk, column.get(), rf_desc, eval_context);

        filter->evaluate(column.get(), &eval_context.running_context);

        auto true_count = SIMD::count_nonzero(selection);
        eval_context.run_filter_nums += 1;

        if (true_count == 0) {
            chunk->set_num_rows(0);
            return;
        } else {
            chunk->filter(selection);
        }
    }
}

void RuntimeFilterProbeCollector::do_evaluate_partial_chunk(Chunk* partial_chunk,
                                                            RuntimeMembershipFilterEvalContext& eval_context) {
    auto& selection = eval_context.running_context.selection;
    eval_context.running_context.use_merged_selection = false;
    eval_context.running_context.compatibility =
            _runtime_state->func_version() <= 3 || !_runtime_state->enable_pipeline_engine();

    // since partial chunk is currently very lightweight (a bunch of const columns), use every runtime filter if possible
    // without computing each rf's selectivity
    for (auto kv : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = kv.second;
        const RuntimeFilter* filter = rf_desc->runtime_filter(eval_context.driver_sequence);
        if (filter == nullptr || filter->always_true()) {
            continue;
        }

        auto only_reference_existent_slots = [&](ExprContext* expr) {
            std::vector<SlotId> slot_ids;
            int n = expr->root()->get_slot_ids(&slot_ids);
            DCHECK(slot_ids.size() == n);

            // do not allow struct subfield
            if (expr->root()->get_subfields(nullptr) > 0) {
                return false;
            }

            for (auto slot_id : slot_ids) {
                if (!partial_chunk->is_slot_exist(slot_id)) {
                    return false;
                }
            }

            return true;
        };

        auto* probe_expr = rf_desc->probe_expr_ctx();
        auto* partition_by_exprs = rf_desc->partition_by_expr_contexts();

        bool can_use_rf_on_partial_chunk = only_reference_existent_slots(probe_expr);
        for (auto* part_by_expr : *partition_by_exprs) {
            can_use_rf_on_partial_chunk &= only_reference_existent_slots(part_by_expr);
        }

        // skip runtime filter that references a non-existent column for the partial chunk
        if (!can_use_rf_on_partial_chunk) {
            continue;
        }

        ColumnPtr column = EVALUATE_NULL_IF_ERROR(probe_expr, probe_expr->root(), partial_chunk);
        // for colocate grf
        compute_hash_values(partial_chunk, column.get(), rf_desc, eval_context);
        filter->evaluate(column.get(), &eval_context.running_context);

        auto true_count = SIMD::count_nonzero(selection);
        eval_context.run_filter_nums += 1;

        if (true_count == 0) {
            partial_chunk->set_num_rows(0);
            return;
        } else {
            partial_chunk->filter(selection);
        }
    }
}

void RuntimeFilterProbeCollector::init_counter() {
    _eval_context.join_runtime_filter_timer = ADD_TIMER(_runtime_profile, "JoinRuntimeFilterTime");
    _eval_context.join_runtime_filter_hash_timer = ADD_TIMER(_runtime_profile, "JoinRuntimeFilterHashTime");
    _eval_context.join_runtime_filter_input_counter =
            ADD_COUNTER(_runtime_profile, "JoinRuntimeFilterInputRows", TUnit::UNIT);
    _eval_context.join_runtime_filter_output_counter =
            ADD_COUNTER(_runtime_profile, "JoinRuntimeFilterOutputRows", TUnit::UNIT);
    _eval_context.join_runtime_filter_eval_counter =
            ADD_COUNTER(_runtime_profile, "JoinRuntimeFilterEvaluate", TUnit::UNIT);
}

void RuntimeFilterProbeCollector::evaluate(Chunk* chunk) {
    if (_descriptors.empty()) return;
    if (_eval_context.join_runtime_filter_timer == nullptr) {
        init_counter();
    }
    evaluate(chunk, _eval_context);
}

void RuntimeFilterProbeCollector::evaluate(Chunk* chunk, RuntimeMembershipFilterEvalContext& eval_context) {
    if (_descriptors.empty()) return;
    size_t before = chunk->num_rows();
    if (before == 0) return;

    {
        SCOPED_TIMER(eval_context.join_runtime_filter_timer);
        eval_context.join_runtime_filter_input_counter->update(before);
        eval_context.run_filter_nums = 0;
        do_evaluate(chunk, eval_context);
        size_t after = chunk->num_rows();
        eval_context.join_runtime_filter_output_counter->update(after);
        eval_context.join_runtime_filter_eval_counter->update(eval_context.run_filter_nums);
    }
}

void RuntimeFilterProbeCollector::evaluate_partial_chunk(Chunk* partial_chunk,
                                                         RuntimeMembershipFilterEvalContext& eval_context) {
    if (_descriptors.empty()) return;
    size_t before = partial_chunk->num_rows();
    if (before == 0) return;

    {
        SCOPED_TIMER(eval_context.join_runtime_filter_timer);
        eval_context.join_runtime_filter_input_counter->update(before);
        eval_context.run_filter_nums = 0;
        do_evaluate_partial_chunk(partial_chunk, eval_context);
        size_t after = partial_chunk->num_rows();
        eval_context.join_runtime_filter_output_counter->update(after);
        eval_context.join_runtime_filter_eval_counter->update(eval_context.run_filter_nums);
    }
}

void RuntimeFilterProbeCollector::compute_hash_values(Chunk* chunk, Column* column,
                                                      RuntimeFilterProbeDescriptor* rf_desc,
                                                      RuntimeMembershipFilterEvalContext& eval_context) {
    // TODO: Hash values will be computed multi times for runtime filters with the same partition_by_exprs.
    SCOPED_TIMER(eval_context.join_runtime_filter_hash_timer);
    const RuntimeFilter* filter = rf_desc->runtime_filter(eval_context.driver_sequence);
    DCHECK(filter);
    if (filter->num_hash_partitions() == 0) {
        return;
    }

    if (rf_desc->partition_by_expr_contexts()->empty()) {
        filter->compute_partition_index(rf_desc->layout(), {column}, &eval_context.running_context);
    } else {
        // Used to hold generated columns
        std::vector<ColumnPtr> column_holders;
        std::vector<const Column*> partition_by_columns;
        for (auto& partition_ctx : *(rf_desc->partition_by_expr_contexts())) {
            ColumnPtr partition_column = EVALUATE_NULL_IF_ERROR(partition_ctx, partition_ctx->root(), chunk);
            partition_by_columns.push_back(partition_column.get());
            column_holders.emplace_back(std::move(partition_column));
        }
        filter->compute_partition_index(rf_desc->layout(), partition_by_columns, &eval_context.running_context);
    }
}

void RuntimeFilterProbeCollector::update_selectivity(Chunk* chunk, RuntimeMembershipFilterEvalContext& eval_context) {
    size_t chunk_size = chunk->num_rows();
    auto& merged_selection = eval_context.running_context.merged_selection;
    auto& use_merged_selection = eval_context.running_context.use_merged_selection;
    eval_context.running_context.compatibility =
            _runtime_state->func_version() <= 3 || !_runtime_state->enable_pipeline_engine();
    auto& seletivity_map = eval_context.selectivity;
    use_merged_selection = true;

    seletivity_map.clear();
    for (auto& kv : _descriptors) {
        RuntimeFilterProbeDescriptor* rf_desc = kv.second;
        const RuntimeFilter* filter = rf_desc->runtime_filter(eval_context.driver_sequence);
        bool should_use = eval_context.mode == RuntimeMembershipFilterEvalContext::Mode::M_ONLY_TOPN &&
                          rf_desc->is_stream_build_filter();
        if (filter == nullptr || (!should_use && filter->always_true())) {
            continue;
        }
        if (eval_context.mode == RuntimeMembershipFilterEvalContext::Mode::M_WITHOUT_TOPN &&
            rf_desc->is_stream_build_filter()) {
            continue;
        } else if (eval_context.mode == RuntimeMembershipFilterEvalContext::Mode::M_ONLY_TOPN &&
                   !rf_desc->is_stream_build_filter()) {
            continue;
        }

        if (rf_desc->has_push_down_to_storage()) {
            continue;
        }
        auto& selection = eval_context.running_context.use_merged_selection
                                  ? eval_context.running_context.merged_selection
                                  : eval_context.running_context.selection;
        auto ctx = rf_desc->probe_expr_ctx();
        ColumnPtr column = EVALUATE_NULL_IF_ERROR(ctx, ctx->root(), chunk);
        // for colocate grf
        compute_hash_values(chunk, column.get(), rf_desc, eval_context);
        // true count is not accummulated, it is evaluated for each RF respectively
        filter->evaluate(column.get(), &eval_context.running_context);
        auto true_count = SIMD::count_nonzero(selection);
        eval_context.run_filter_nums += 1;
        double selectivity = true_count * 1.0 / chunk_size;
        if (selectivity <= 0.5) {                          // useful filter
            if (selectivity < _early_return_selectivity) { // very useful filter, could early return
                seletivity_map.clear();
                seletivity_map.emplace(selectivity, rf_desc);
                chunk->filter(selection);
                return;
            }

            // Only choose three most selective runtime filters
            if (seletivity_map.size() < 3) {
                seletivity_map.emplace(selectivity, rf_desc);
            } else {
                auto it = seletivity_map.end();
                it--;
                if (selectivity < it->first) {
                    seletivity_map.erase(it);
                    seletivity_map.emplace(selectivity, rf_desc);
                }
            }

            if (use_merged_selection) {
                use_merged_selection = false;
            } else {
                uint8_t* dest = merged_selection.data();
                const uint8_t* src = selection.data();
                for (size_t j = 0; j < chunk_size; ++j) {
                    dest[j] = src[j] & dest[j];
                }
            }
        } else if (rf_desc->is_stream_build_filter() &&
                   eval_context.mode == RuntimeMembershipFilterEvalContext::Mode::M_ONLY_TOPN) {
            seletivity_map.emplace(selectivity, rf_desc);
        }
    }
    if (!seletivity_map.empty()) {
        chunk->filter(merged_selection);
    }
}

static bool contains_dict_mapping_expr(Expr* expr) {
    if (typeid(*expr) == typeid(DictMappingExpr)) {
        return true;
    }

    return std::any_of(expr->children().begin(), expr->children().end(),
                       [](Expr* child) { return contains_dict_mapping_expr(child); });
}

static bool contains_dict_mapping_expr(RuntimeFilterProbeDescriptor* probe_desc) {
    auto* probe_expr_ctx = probe_desc->probe_expr_ctx();
    if (probe_expr_ctx == nullptr) {
        return false;
    }
    return contains_dict_mapping_expr(probe_expr_ctx->root());
}

void RuntimeFilterProbeCollector::push_down(const RuntimeState* state, TPlanNodeId target_plan_node_id,
                                            RuntimeFilterProbeCollector* parent, const std::vector<TupleId>& tuple_ids,
                                            std::set<TPlanNodeId>& local_rf_waiting_set) {
    if (this == parent) return;
    auto iter = parent->_descriptors.begin();
    while (iter != parent->_descriptors.end()) {
        RuntimeFilterProbeDescriptor* desc = iter->second;
        if (!desc->can_push_down_runtime_filter()) {
            ++iter;
            continue;
        }
        if (desc->is_bound(tuple_ids) &&
            !(state->broadcast_join_right_offsprings().contains(target_plan_node_id) &&
              state->non_broadcast_rf_ids().contains(desc->filter_id())) &&
            !contains_dict_mapping_expr(desc)) {
            add_descriptor(desc);
            if (desc->is_local()) {
                local_rf_waiting_set.insert(desc->build_plan_node_id());
            }
            iter = parent->_descriptors.erase(iter);
        } else {
            ++iter;
        }
    }
}

std::string RuntimeFilterProbeCollector::debug_string() const {
    std::stringstream ss;
    ss << "RFColl(";
    for (auto& it : _descriptors) {
        RuntimeFilterProbeDescriptor* desc = it.second;
        if (desc != nullptr) {
            ss << "[" << desc->debug_string() << "]";
        }
    }
    ss << ")";
    return ss.str();
}

void RuntimeFilterProbeCollector::add_descriptor(RuntimeFilterProbeDescriptor* desc) {
    _descriptors[desc->filter_id()] = desc;
}

// only used in non-pipeline mode
void RuntimeFilterProbeCollector::wait(bool on_scan_node) {
    if (_descriptors.empty()) return;

    std::list<RuntimeFilterProbeDescriptor*> wait_list;
    for (auto& it : _descriptors) {
        auto* rf = it.second;
        int filter_id = rf->filter_id();
        VLOG_FILE << "RuntimeFilterCollector::wait start. filter_id = " << filter_id
                  << ", plan_node_id = " << _plan_node_id << ", finst_id = " << _runtime_state->fragment_instance_id();
        wait_list.push_back(it.second);
    }

    int wait_time = _wait_timeout_ms;
    if (on_scan_node) {
        wait_time = _scan_wait_timeout_ms;
    }
    const int wait_interval = 5;
    auto wait_duration = std::chrono::milliseconds(wait_interval);
    while (wait_time >= 0 && !wait_list.empty()) {
        auto it = wait_list.begin();
        while (it != wait_list.end()) {
            auto* rf = (*it)->runtime_filter(-1);
            // find runtime filter in cache.
            if (rf == nullptr) {
                RuntimeFilterPtr t = _runtime_state->exec_env()->runtime_filter_cache()->get(_runtime_state->query_id(),
                                                                                             (*it)->filter_id());
                if (t != nullptr) {
                    VLOG_FILE << "RuntimeFilterCollector::wait: rf found in cache. filter_id = " << (*it)->filter_id()
                              << ", plan_node_id = " << _plan_node_id
                              << ", finst_id  = " << _runtime_state->fragment_instance_id();
                    (*it)->set_shared_runtime_filter(t);
                    rf = t.get();
                }
            }
            if (rf != nullptr) {
                it = wait_list.erase(it);
            } else {
                ++it;
            }
        }
        if (wait_list.empty()) break;
        std::this_thread::sleep_for(wait_duration);
        wait_time -= wait_interval;
    }

    if (_descriptors.size() != 0) {
        for (const auto& it : _descriptors) {
            auto* rf = it.second;
            int filter_id = rf->filter_id();
            bool ready = (rf->runtime_filter(-1) != nullptr);
            VLOG_FILE << "RuntimeFilterCollector::wait start. filter_id = " << filter_id
                      << ", plan_node_id = " << _plan_node_id
                      << ", finst_id = " << _runtime_state->fragment_instance_id()
                      << ", ready = " << std::to_string(ready);
        }
    }
}

void RuntimeFilterProbeDescriptor::set_runtime_filter(const RuntimeFilter* rf) {
    auto notify = DeferOp([this]() { _observable.notify_source_observers(); });
    const RuntimeFilter* expected = nullptr;
    _runtime_filter.compare_exchange_strong(expected, rf, std::memory_order_seq_cst, std::memory_order_seq_cst);
    if (_ready_timestamp == 0 && rf != nullptr && _latency_timer != nullptr) {
        _ready_timestamp = UnixMillis();
        _latency_timer->set((_ready_timestamp - _open_timestamp) * 1000);
    }
}

void RuntimeFilterProbeDescriptor::set_shared_runtime_filter(const std::shared_ptr<const RuntimeFilter>& rf) {
    std::shared_ptr<const RuntimeFilter> old_value = nullptr;
    if (std::atomic_compare_exchange_strong(&_shared_runtime_filter, &old_value, rf)) {
        set_runtime_filter(_shared_runtime_filter.get());
    }
}

void RuntimeFilterHelper::create_min_max_value_predicate(ObjectPool* pool, SlotId slot_id, LogicalType slot_type,
                                                         const RuntimeFilter* filter, Expr** min_max_predicate) {
    *min_max_predicate = nullptr;
    if (filter == nullptr) return;
    if (filter->get_min_max_filter() == nullptr) return;
    // TODO, if you want to enable it for string, pls adapt for low-cardinality string
    if (slot_type == TYPE_CHAR || slot_type == TYPE_VARCHAR) return;
    auto res = type_dispatch_filter(slot_type, (Expr*)nullptr, MinMaxPredicateBuilder(pool, slot_id, filter));
    *min_max_predicate = res;
}

} // namespace starrocks
