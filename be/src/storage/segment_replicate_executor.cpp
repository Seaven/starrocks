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

#include "storage/segment_replicate_executor.h"

#include <fmt/format.h>

#include <memory>
#include <utility>

#include "fs/fs_posix.h"
#include "gen_cpp/data.pb.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "runtime/load_fail_point.h"
#include "runtime/mem_tracker.h"
#include "storage/delta_writer.h"
#include "util/brpc_stub_cache.h"
#include "util/raw_container.h"

namespace starrocks {

class SegmentReplicateTask final : public Runnable {
public:
    SegmentReplicateTask(ReplicateToken* replicate_token, std::unique_ptr<SegmentPB> segment, bool eos)
            : _replicate_token(replicate_token),
              _segment(std::move(segment)),
              _eos(eos),
              _create_time_ns(MonotonicNanos()) {}

    ~SegmentReplicateTask() override = default;

    void run() override {
        auto& stat = _replicate_token->_stat;
        stat.num_pending_tasks.fetch_add(-1, std::memory_order_relaxed);
        stat.pending_time_ns.fetch_add(MonotonicNanos() - _create_time_ns, std::memory_order_relaxed);
        stat.num_running_tasks.fetch_add(1, std::memory_order_relaxed);
        int64_t duration_ns = 0;
        {
            SCOPED_RAW_TIMER(&duration_ns);
            _replicate_token->_sync_segment(std::move(_segment), _eos);
        }
        stat.num_running_tasks.fetch_add(-1, std::memory_order_relaxed);
        stat.num_finished_tasks.fetch_add(1, std::memory_order_relaxed);
        stat.execute_time_ns.fetch_add(duration_ns, std::memory_order_relaxed);
    }

private:
    ReplicateToken* _replicate_token;
    std::unique_ptr<SegmentPB> _segment;
    bool _eos;
    int64_t _create_time_ns;
};

ReplicateChannel::ReplicateChannel(const DeltaWriterOptions* opt, std::string host, int32_t port, int64_t node_id)
        : _opt(opt), _host(std::move(host)), _port(port), _node_id(node_id) {
    _closure = new ReusableClosure<PTabletWriterAddSegmentResult>();
    _closure->ref();
}

ReplicateChannel::~ReplicateChannel() {
    if (_closure != nullptr) {
        _closure->join();
        if (_closure->unref()) {
            delete _closure;
        }
        _closure = nullptr;
    }
}

std::string ReplicateChannel::debug_string() {
    return fmt::format("SyncChannnel [host: {}, port: {}, load_id: {}, tablet_id: {}, txn_id: {}]", _host, _port,
                       print_id(_opt->load_id), _opt->tablet_id, _opt->txn_id);
}

Status ReplicateChannel::_init() {
    if (_inited) {
        return Status::OK();
    }
    _inited = true;

    _stub = ExecEnv::GetInstance()->brpc_stub_cache()->get_stub(_host, _port);
    if (_stub == nullptr) {
        auto msg = fmt::format("Failed to Connect {} failed.", debug_string().c_str());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    _mem_tracker = std::make_unique<MemTracker>(-1, "replicate: " + UniqueId(_opt->load_id).to_string(),
                                                GlobalEnv::GetInstance()->load_mem_tracker());
    if (!_mem_tracker) {
        auto msg = fmt::format("Failed to get load mem tracker for {} failed.", debug_string().c_str());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    return Status::OK();
}

Status ReplicateChannel::async_segment(SegmentPB* segment, butil::IOBuf& data, bool eos,
                                       std::vector<std::unique_ptr<PTabletInfo>>* replicate_tablet_infos,
                                       std::vector<std::unique_ptr<PTabletInfo>>* failed_tablet_infos) {
    RETURN_IF_ERROR(get_status());

    VLOG(2) << "Async tablet " << _opt->tablet_id << " segment id " << (segment == nullptr ? -1 : segment->segment_id())
            << " eos " << eos << " to [" << _host << ":" << _port;

    // 1. init sync channel
    Status status = _init();
    if (!status.ok()) {
        _set_status(status);
        return status;
    }

    // 2. wait pre request's result
    RETURN_IF_ERROR(_wait_response(replicate_tablet_infos, failed_tablet_infos));

    // 3. send segment sync request
    _send_request(segment, data, eos);

    // 4. wait if eos=true
    if (eos || _mem_tracker->any_limit_exceeded()) {
        RETURN_IF_ERROR(_wait_response(replicate_tablet_infos, failed_tablet_infos));
    }

    VLOG(2) << "Asynced tablet " << _opt->tablet_id << " segment id "
            << (segment == nullptr ? -1 : segment->segment_id()) << " eos " << eos << " to [" << _host << ":" << _port
            << "] res " << _closure->result.DebugString();

    return get_status();
}

void ReplicateChannel::_send_request(SegmentPB* segment, butil::IOBuf& data, bool eos) {
    PTabletWriterAddSegmentRequest request;
    request.set_allocated_id(const_cast<starrocks::PUniqueId*>(&_opt->load_id));
    request.set_tablet_id(_opt->tablet_id);
    request.set_eos(eos);
    request.set_txn_id(_opt->txn_id);
    request.set_index_id(_opt->index_id);
    request.set_sink_id(_opt->sink_id);

    VLOG(2) << "Send segment to " << debug_string()
            << " segment_id=" << (segment == nullptr ? -1 : segment->segment_id()) << " eos=" << eos
            << " txn_id=" << _opt->txn_id << " index_id=" << _opt->index_id << " sink_id=" << _opt->sink_id;

    _closure->ref();
    _closure->reset();
    _closure->cntl.set_timeout_ms(_opt->timeout_ms);
    SET_IGNORE_OVERCROWDED(_closure->cntl, load);

    if (segment != nullptr) {
        request.set_allocated_segment(segment);
        _closure->cntl.request_attachment().append(data);
    }
    _closure->request_size = _closure->cntl.request_attachment().size();

    // brpc send buffer is also considered as part of the memory used by load
    _mem_tracker->consume_without_root(_closure->request_size);

    FAIL_POINT_TRIGGER_EXECUTE(load_tablet_writer_add_segment,
                               TABLET_WRITER_ADD_SEGMENT_FP_ACTION(_host, _closure, request));
    _stub->tablet_writer_add_segment(&_closure->cntl, &request, &_closure->result, _closure);

    request.release_id();
    if (segment != nullptr) {
        request.release_segment();
    }
}

Status ReplicateChannel::_wait_response(std::vector<std::unique_ptr<PTabletInfo>>* replicate_tablet_infos,
                                        std::vector<std::unique_ptr<PTabletInfo>>* failed_tablet_infos) {
    if (_closure->join()) {
        Status status;
        _mem_tracker->release_without_root(_closure->request_size);
        if (_closure->cntl.Failed()) {
            status = Status::InternalError(_closure->cntl.ErrorText());
            LOG(WARNING) << "Failed to send rpc to " << debug_string() << " err=" << status;
            _set_status(status);
            return status;
        }
        status = _closure->result.status();
        if (!status.ok()) {
            LOG(WARNING) << "Failed to send rpc to " << debug_string() << " err=" << status;
            _set_status(status);
            return status;
        }

        for (size_t i = 0; i < _closure->result.tablet_vec_size(); ++i) {
            replicate_tablet_infos->emplace_back(std::make_unique<PTabletInfo>());
            replicate_tablet_infos->back()->Swap(_closure->result.mutable_tablet_vec(i));
        }

        for (size_t i = 0; i < _closure->result.failed_tablet_vec_size(); ++i) {
            failed_tablet_infos->emplace_back(std::make_unique<PTabletInfo>());
            failed_tablet_infos->back()->Swap(_closure->result.mutable_failed_tablet_vec(i));
        }
    }

    return Status::OK();
}

void ReplicateChannel::cancel(const Status& status) {
    // cancel rpc request, accelerate the release of related resources
    // Cancel an already-cancelled call_id has no effect.
    _closure->cancel();
    _set_status(status);
}

ReplicateToken::ReplicateToken(std::unique_ptr<ThreadPoolToken> replicate_pool_token, const DeltaWriterOptions* opt)
        : _replicate_token(std::move(replicate_pool_token)), _status(), _opt(opt), _fs(new_fs_posix()) {
    // first replica is primary replica, skip it
    for (size_t i = 1; i < opt->replicas.size(); ++i) {
        auto node_id = opt->replicas[i].node_id();
        _replicate_channels.emplace(node_id, std::make_unique<ReplicateChannel>(opt, opt->replicas[i].host(),
                                                                                opt->replicas[i].port(), node_id));
        _replica_node_ids.emplace_back(node_id);
    }
    if (opt->write_quorum == WriteQuorumTypePB::ONE) {
        _max_fail_replica_num = opt->replicas.size();
    } else if (opt->write_quorum == WriteQuorumTypePB::ALL) {
        _max_fail_replica_num = 0;
    } else {
        _max_fail_replica_num = opt->replicas.size() - (opt->replicas.size() / 2 + 1);
    }
}

Status ReplicateToken::submit(std::unique_ptr<SegmentPB> segment, bool eos) {
    RETURN_IF_ERROR(status());
    if (segment == nullptr && !eos) {
        return Status::InternalError(fmt::format("{} segment=null eos=false", debug_string()));
    }
    auto task = std::make_shared<SegmentReplicateTask>(this, std::move(segment), eos);
    Status st = _replicate_token->submit(std::move(task));
    if (st.ok()) {
        _stat.num_pending_tasks.fetch_add(1, std::memory_order_relaxed);
    }
    return st;
}

void ReplicateToken::cancel(const Status& st) {
    set_status(st);
}

void ReplicateToken::shutdown() {
    _replicate_token->shutdown();
}

Status ReplicateToken::wait() {
    _replicate_token->wait();
    std::lock_guard l(_status_lock);
    return _status;
}

Status ReplicateToken::get_replica_status(int64_t node_id) const {
    auto channel = _replicate_channels.find(node_id);
    if (channel == _replicate_channels.end()) {
        return Status::NotFound("replica node id not found");
    }
    return channel->second->get_status();
}

std::string ReplicateToken::debug_string() {
    return fmt::format("[ReplicateToken tablet_id: {}, txn_id: {}]", _opt->tablet_id, _opt->txn_id);
}

void ReplicateToken::_sync_segment(std::unique_ptr<SegmentPB> segment, bool eos) {
    // If previous sync has failed, return directly
    if (!status().ok()) return;

    // 1. read segment from local storage
    butil::IOBuf data;
    if (segment) {
        // 1.1 read segment file
        if (segment->has_path()) {
            auto res = _fs->new_random_access_file(segment->path());
            if (!res.ok()) {
                LOG(WARNING) << "Failed to open segment file " << segment->DebugString() << " by " << debug_string()
                             << " err " << res.status();
                return set_status(res.status());
            }
            auto rfile = std::move(res.value());
            auto buf = new uint8[segment->data_size()];
            data.append_user_data(buf, segment->data_size(), [](void* buf) { delete[](uint8*) buf; });
            auto st = rfile->read_fully(buf, segment->data_size());
            if (!st.ok()) {
                LOG(WARNING) << "Failed to read segment " << segment->DebugString() << " by " << debug_string()
                             << " err " << st;
                return set_status(st);
            }
        }
        if (segment->has_delete_path()) {
            auto res = _fs->new_random_access_file(segment->delete_path());
            if (!res.ok()) {
                LOG(WARNING) << "Failed to open delete file " << segment->DebugString() << " by " << debug_string()
                             << " err " << res.status();
                return set_status(res.status());
            }
            auto rfile = std::move(res.value());
            auto buf = new uint8[segment->delete_data_size()];
            data.append_user_data(buf, segment->delete_data_size(), [](void* buf) { delete[](uint8*) buf; });
            auto st = rfile->read_fully(buf, segment->delete_data_size());
            if (!st.ok()) {
                LOG(WARNING) << "Failed to read delete file " << segment->DebugString() << " by " << debug_string()
                             << " err " << st;
                return set_status(st);
            }
        }
        if (segment->has_update_path()) {
            auto res = _fs->new_random_access_file(segment->update_path());
            if (!res.ok()) {
                LOG(WARNING) << "Failed to open update file " << segment->DebugString() << " by " << debug_string()
                             << " err " << res.status();
                return set_status(res.status());
            }
            auto rfile = std::move(res.value());
            auto buf = new uint8[segment->update_data_size()];
            data.append_user_data(buf, segment->update_data_size(), [](void* buf) { delete[](uint8*) buf; });
            auto st = rfile->read_fully(buf, segment->update_data_size());
            if (!st.ok()) {
                LOG(WARNING) << "Failed to read delete file " << segment->DebugString() << " by " << debug_string()
                             << " err " << st;
                return set_status(st);
            }
        }
        if (!segment->seg_indexes().empty()) {
            auto mutable_indexes = segment->mutable_seg_indexes();
            size_t total_index_data_size = 0;
            for (int i = 0; i < mutable_indexes->size(); i++) {
                auto& index = mutable_indexes->at(i);
                if (index.index_type() == VECTOR) {
                    auto index_path = mutable_indexes->at(i).index_path();
                    auto res = _fs->new_random_access_file(index_path);

                    if (!res.ok()) {
                        LOG(WARNING) << "Failed to open index file " << index_path << " by " << debug_string()
                                     << " err " << res.status();
                        return set_status(res.status());
                    }

                    auto file_size_res = _fs->get_file_size(index_path);
                    if (!file_size_res.ok()) {
                        LOG(WARNING) << "Failed to get index file size " << index_path << " err " << res.status();
                        return set_status(res.status());
                    }
                    auto file_size = file_size_res.value();
                    mutable_indexes->at(i).set_index_file_size(file_size);
                    total_index_data_size += file_size;

                    auto rfile = std::move(res.value());
                    auto buf = new uint8[file_size];
                    data.append_user_data(buf, file_size, [](void* buf) { delete[](uint8*) buf; });
                    auto st = rfile->read_fully(buf, file_size);
                    if (!st.ok()) {
                        LOG(WARNING) << "Failed to read index file " << segment->DebugString() << " by "
                                     << debug_string() << " err " << st;
                        return set_status(st);
                    }
                }
                segment->set_seg_index_data_size(total_index_data_size);
            }
        }
    }

    // 2. send segment to secondary replica
    for (const auto& [_, channel] : _replicate_channels) {
        auto st = Status::OK();
        if (_failed_node_id.count(channel->node_id()) == 0) {
            st = channel->async_segment(segment.get(), data, eos, &_replicated_tablet_infos, &_failed_tablet_infos);
            if (!st.ok()) {
                LOG(WARNING) << "Failed to sync segment " << channel->debug_string() << " err " << st;
                channel->cancel(st);
                _failed_node_id.insert(channel->node_id());
            }
        }

        if (_failed_node_id.size() > _max_fail_replica_num) {
            LOG(WARNING) << "Failed to sync segment err " << st << " by " << debug_string() << " fail_num "
                         << _failed_node_id.size() << " max_fail_num " << _max_fail_replica_num;
            for (const auto& [_, channel] : _replicate_channels) {
                if (_failed_node_id.count(channel->node_id()) == 0) {
                    channel->cancel(Status::InternalError("failed replica num exceed max fail num"));
                }
            }
            return set_status(st);
        }
    }
}

Status SegmentReplicateExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int data_dir_num = static_cast<int>(data_dirs.size());
    int min_threads = std::max<int>(1, config::flush_thread_num_per_store);
    int max_threads = std::max(data_dir_num * min_threads, min_threads);
    return ThreadPoolBuilder("segment_replicate")
            .set_min_threads(min_threads)
            .set_max_threads(max_threads)
            .build(&_replicate_pool);
}

Status SegmentReplicateExecutor::update_max_threads(int max_threads) {
    if (_replicate_pool != nullptr) {
        return _replicate_pool->update_max_threads(max_threads);
    } else {
        return Status::InternalError("Thread pool not exist");
    }
}

std::unique_ptr<ReplicateToken> SegmentReplicateExecutor::create_replicate_token(
        const DeltaWriterOptions* opt, ThreadPool::ExecutionMode execution_mode) {
    return std::make_unique<ReplicateToken>(_replicate_pool->new_token(execution_mode), opt);
}

} // namespace starrocks
