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

#include "exec/pipeline/result_sink_operator.h"

#include <arrow/type.h>

#include "exprs/expr.h"
#include "runtime/arrow_result_writer.h"
#include "runtime/buffer_control_block.h"
#include "runtime/customized_result_writer.h"
#include "runtime/http_result_writer.h"
#include "runtime/metadata_result_writer.h"
#include "runtime/mysql_result_writer.h"
#include "runtime/query_statistics.h"
#include "runtime/result_buffer_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/statistic_result_writer.h"
#include "runtime/variable_result_writer.h"

namespace starrocks::pipeline {
Status ResultSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));

    // Create profile
    _unique_metrics->add_info_string("SinkType", to_string(_sink_type));
    auto profile = _unique_metrics.get();
    _sender->attach_query_ctx(state->query_ctx()->get_shared_ptr());
    _sender->attach_observer(state, observer());

    // Create writer based on sink type
    switch (_sink_type) {
    case TResultSinkType::MYSQL_PROTOCAL:
        _writer = std::make_shared<MysqlResultWriter>(_sender.get(), _output_expr_ctxs, _is_binary_format, profile);
        break;
    case TResultSinkType::STATISTIC:
        _writer = std::make_shared<StatisticResultWriter>(_sender.get(), _output_expr_ctxs, profile);
        break;
    case TResultSinkType::VARIABLE:
        _writer = std::make_shared<VariableResultWriter>(_sender.get(), _output_expr_ctxs, profile);
        break;
    case TResultSinkType::HTTP_PROTOCAL:
        _writer = std::make_shared<HttpResultWriter>(_sender.get(), _output_expr_ctxs, profile, _format_type);
        break;
    case TResultSinkType::METADATA_ICEBERG:
        _writer = std::make_shared<MetadataResultWriter>(_sender.get(), _output_expr_ctxs, profile, _sink_type);
        break;
    case TResultSinkType::CUSTOMIZED:
        // CustomizedResultWriter is a general-purposed result writer that used by FE to executing internal
        // query, the result is serialized in packed binary format. FE can parse the result row into object
        // via ORM(Object-Relation Mapping) mechanism. In the future, all the internal queries should be
        // unified to adopt this result sink type.
        _writer = std::make_shared<CustomizedResultWriter>(_sender.get(), _output_expr_ctxs, profile);
        break;
    case TResultSinkType::ARROW_FLIGHT_PROTOCAL:
        _writer = std::make_shared<ArrowResultWriter>(_sender.get(), _output_expr_ctxs, profile, _row_desc);
        break;
    default:
        return Status::InternalError("Unknown result sink type");
    }

    RETURN_IF_ERROR(_writer->init(state));
    return Status::OK();
}

void ResultSinkOperator::close(RuntimeState* state) {
    Status st;
    // Close the writer
    if (_writer != nullptr) {
        st = _writer->close();
        _num_written_rows.fetch_add(_writer->get_written_rows(), std::memory_order_relaxed);
    }

    // Close the shared sender when the last result sink operator is closing.
    if (_num_sinkers.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        if (_sender != nullptr) {
            // Incrementing and reading _num_written_rows needn't memory barrier, because
            // the visibility of _num_written_rows is guaranteed by _num_sinkers.fetch_sub().
            _sender->update_num_written_rows(_num_written_rows.load(std::memory_order_relaxed));

            QueryContext* query_ctx = state->query_ctx();
            auto query_statistic = query_ctx->final_query_statistic();
            query_statistic->set_returned_rows(_num_written_rows);
            _sender->set_query_statistics(query_statistic);

            Status final_status = _fragment_ctx->final_status();
            if (!st.ok() && final_status.ok()) {
                final_status = st;
            }
            WARN_IF_ERROR(_sender->close(final_status), "close sender failed");
        }

        (void)state->exec_env()->result_mgr()->cancel_at_time(
                time(nullptr) + config::result_buffer_cancelled_interval_time, state->fragment_instance_id());
    }

    Operator::close(state);
}

StatusOr<ChunkPtr> ResultSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Shouldn't pull chunk from result sink operator");
}

Status ResultSinkOperator::set_cancelled(RuntimeState* state) {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);
    _writer->cancel();
    return Status::OK();
}

bool ResultSinkOperator::need_input() const {
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);

    if (is_finished()) {
        return false;
    }

    return !_writer->is_full();
}

Status ResultSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    // The ResultWriter memory that sends the results is no longer recorded to the query memory.
    // There are two reason:
    // 1. the query result has come out, and then the memory limit is triggered, cancel, it is not necessary
    // 2. if this memory is counted, The memory of the receiving thread needs to be recorded,
    // and the life cycle of MemTracker needs to be considered
    //
    // All the places where acquire and release memory of _fetch_data_result must use process_mem_tracker.
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(nullptr);

    if (!_last_error.ok()) {
        return _last_error;
    }

    return _writer->add_to_write_buffer(chunk.get());
}

Status ResultSinkOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(OperatorFactory::prepare(state));
    RETURN_IF_ERROR(state->exec_env()->result_mgr()->create_sender(state->fragment_instance_id(),
                                                                   std::min<int>(_dop << 1, 1024), &_sender));

    RETURN_IF_ERROR(Expr::create_expr_trees(state->obj_pool(), _t_output_expr, &_output_expr_ctxs, state));

    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    return Status::OK();
}

void ResultSinkOperatorFactory::close(RuntimeState* state) {
    if (_sender != nullptr) {
        WARN_IF_ERROR(_sender->close(_fragment_ctx->final_status()), "close sender failed");
    }

    Expr::close(_output_expr_ctxs, state);
    OperatorFactory::close(state);
}

} // namespace starrocks::pipeline
