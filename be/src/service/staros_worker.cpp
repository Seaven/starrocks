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

#ifdef USE_STAROS
#include "service/staros_worker.h"

#include <fslib/fslib_all_initializer.h>
#include <starlet.h>
#include <worker.h>

#include "common/config.h"
#include "common/gflags_utils.h"
#include "common/logging.h"
#include "common/shutdown_hook.h"
#include "file_store.pb.h"
#include "fmt/format.h"
#include "fslib/star_cache_configuration.h"
#include "fslib/star_cache_handler.h"
#include "gflags/gflags.h"
#include "util/await.h"
#include "util/debug_util.h"
#include "util/defer_op.h"
#include "util/lru_cache.h"
#include "util/sha.h"
#include "util/starrocks_metrics.h"

// cachemgr thread pool size
DECLARE_int32(cachemgr_threadpool_size);
// buffer size in starlet fs buffer stream, size <= 0 means not use buffer stream.
DECLARE_int32(fs_stream_buffer_size_bytes);
// domain allow list to force starlet using s3 virtual address style
DECLARE_string(fslib_s3_virtual_address_domainlist);
// s3client factory cache capacity
DECLARE_int32(fslib_s3client_max_items);
// s3client max connections
DECLARE_int32(fslib_s3client_max_connections);
// s3client max instances per cache item, allow using multiple client instances per cache
DECLARE_int32(fslib_s3client_max_instance_per_item);
DECLARE_int32(fslib_s3client_nonread_max_retries);
DECLARE_int32(fslib_s3client_nonread_retry_scale_factor);
DECLARE_int32(fslib_s3client_connect_timeout_ms);
DECLARE_int32(fslib_s3client_request_timeout_ms);
DECLARE_bool(fslib_s3client_use_list_objects_v1);
// threadpool size for buffer prefetch task
DECLARE_int32(fs_buffer_prefetch_threadpool_size);
// switch to turn on/off buffer prefetch when read
DECLARE_bool(fs_enable_buffer_prefetch);

namespace starrocks {

std::shared_ptr<StarOSWorker> g_worker;
std::unique_ptr<staros::starlet::Starlet> g_starlet;

namespace fslib = staros::starlet::fslib;

StarOSWorker::StarOSWorker()
        : _mtx(),
          _cache_mtx(),
          _shards(),
          _fs_cache(new_lru_cache(config::starlet_filesystem_instance_cache_capacity)) {}

StarOSWorker::~StarOSWorker() = default;

static const uint64_t kUnknownTableId = UINT64_MAX;

void StarOSWorker::set_fs_cache_capacity(int32_t capacity) {
    _fs_cache->set_capacity(capacity);
}

uint64_t StarOSWorker::get_table_id(const ShardInfo& shard) {
    const auto& properties = shard.properties;
    auto iter = properties.find("tableId");
    if (iter == properties.end()) {
        DCHECK(false) << "tableId doesn't exist in shard properties";
        return kUnknownTableId;
    }
    const auto& tableId = properties.at("tableId");
    try {
        return std::stoull(tableId);
    } catch (const std::exception& e) {
        DCHECK(false) << "failed to parse tableId: " << tableId << ", " << e.what();
        return kUnknownTableId;
    }
}

absl::Status StarOSWorker::add_shard(const ShardInfo& shard) {
    std::unique_lock l(_mtx);
    auto it = _shards.find(shard.id);
    if (it != _shards.end() && it->second.shard_info.path_info.has_fs_info() && shard.path_info.has_fs_info() &&
        it->second.shard_info.path_info.fs_info().version() < shard.path_info.fs_info().version()) {
        auto st = invalidate_fs(it->second.shard_info);
        if (!st.ok()) {
            return st;
        }
    }
    auto ret = _shards.insert_or_assign(shard.id, ShardInfoDetails(shard));
    l.unlock();
    if (ret.second) {
#ifndef BE_TEST
        StarRocksMetrics::instance()->table_metrics_mgr()->register_table(get_table_id(shard));
#endif
        // it is an insert op to the map
        // NOTE:
        //  1. Since the following statement is invoked outside the lock, it is possible that
        //     the shard may be removed when retrieving from the callback.
        //  2. Expect the callback is as quick and simple as possible, otherwise it will occupy
        //     the GRPC thread too long and blocking response sent back to StarManager. A better
        //     choice would be: starting a new thread pool and send callback tasks in the thread pool.
        on_add_shard_event(shard.id);
    }
    return absl::OkStatus();
}

absl::Status StarOSWorker::invalidate_fs(const ShardInfo& info) {
    auto scheme = build_scheme_from_shard_info(info);
    if (!scheme.ok()) {
        return scheme.status();
    }
    auto conf = build_conf_from_shard_info(info);
    if (!conf.ok()) {
        return conf.status();
    }
    erase_fs_cache(get_cache_key(*scheme, *conf));
    return absl::OkStatus();
}

absl::Status StarOSWorker::remove_shard(const ShardId id) {
    std::unique_lock l(_mtx);
    auto iter = _shards.find(id);
    if (iter != _shards.end()) {
#ifndef BE_TEST
        uint64_t table_id = get_table_id(iter->second.shard_info);
        StarRocksMetrics::instance()->table_metrics_mgr()->unregister_table(table_id);
#endif
        _shards.erase(iter);
    }
    return absl::OkStatus();
}

absl::StatusOr<staros::starlet::ShardInfo> StarOSWorker::get_shard_info(ShardId id) const {
    std::shared_lock l(_mtx);
    auto it = _shards.find(id);
    if (it == _shards.end()) {
        return absl::NotFoundError(fmt::format("failed to get shardinfo {}", id));
    }
    return it->second.shard_info;
}

absl::StatusOr<staros::starlet::ShardInfo> StarOSWorker::retrieve_shard_info(ShardId id) {
    auto st = get_shard_info(id);
    if (absl::IsNotFound(st.status())) {
        return _fetch_shard_info_from_remote(id);
    }
    return st;
}

std::vector<staros::starlet::ShardInfo> StarOSWorker::shards() const {
    std::vector<staros::starlet::ShardInfo> vec;
    vec.reserve(_shards.size());

    std::shared_lock l(_mtx);
    for (const auto& shard : _shards) {
        vec.emplace_back(shard.second.shard_info);
    }
    return vec;
}

absl::StatusOr<staros::starlet::WorkerInfo> StarOSWorker::worker_info() const {
    staros::starlet::WorkerInfo worker_info;

    std::shared_lock l(_mtx);
    worker_info.worker_id = worker_id();
    worker_info.service_id = service_id();
    worker_info.properties["port"] = std::to_string(config::starlet_port);
    worker_info.properties["be_port"] = std::to_string(config::be_port);
    worker_info.properties["be_http_port"] = std::to_string(config::be_http_port);
    worker_info.properties["be_brpc_port"] = std::to_string(config::brpc_port);
    worker_info.properties["be_heartbeat_port"] = std::to_string(config::heartbeat_service_port);
    worker_info.properties["be_version"] = get_short_version();
    for (auto& iter : _shards) {
        worker_info.shards.insert(iter.first);
    }
    return worker_info;
}

absl::Status StarOSWorker::update_worker_info(const staros::starlet::WorkerInfo& new_worker_info) {
    return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<fslib::FileSystem>> StarOSWorker::get_shard_filesystem(ShardId id,
                                                                                      const Configuration& conf) {
    ShardInfo shard_info;
    { // shared_lock, check if the filesystem already created
        std::shared_lock l(_mtx);
        auto it = _shards.find(id);
        if (it == _shards.end()) {
            // unlock the lock and try best to build the filesystem with remote rpc call
            l.unlock();
            return build_filesystem_on_demand(id, conf);
        }

        auto fs = lookup_fs_cache(it->second.fs_cache_key);
        if (fs != nullptr) {
            return fs;
        }
        shard_info = it->second.shard_info;
    }

    // Build the filesystem under no lock, so the op won't hold the lock for a long time.
    // It is possible that multiple filesystems are built for the same shard from multiple threads under no lock here.
    auto fs_or = build_filesystem_from_shard_info(shard_info, conf);
    if (!fs_or.ok()) {
        return fs_or.status();
    }
    {
        std::unique_lock l(_mtx);
        auto shard_iter = _shards.find(id);
        // could be possibly shards removed or fs get created during unlock-lock
        if (shard_iter == _shards.end()) {
            return fs_or->second;
        }

        auto fs = lookup_fs_cache(shard_iter->second.fs_cache_key);
        if (fs != nullptr) {
            return fs;
        }

        shard_iter->second.fs_cache_key = std::move(fs_or->first);
        return fs_or->second;
    }
}

absl::StatusOr<staros::starlet::ShardInfo> StarOSWorker::_fetch_shard_info_from_remote(ShardId id) {
    static const int64_t kGetShardInfoTimeout = 5 * 1000 * 1000; // 5s (heartbeat interval)
    static const int64_t kCheckInterval = 10 * 1000;             // 10ms
    Awaitility wait;
    auto cond = []() { return g_starlet->is_ready(); };
    auto ret = wait.timeout(kGetShardInfoTimeout).interval(kCheckInterval).until(cond);
    if (!ret) {
        return absl::UnavailableError("starlet is still not ready!");
    }

    // get_shard_info call will probably trigger an add_shard() call to worker itself. Be sure there is no dead lock.
    return g_starlet->get_shard_info(id);
}

absl::StatusOr<std::shared_ptr<fslib::FileSystem>> StarOSWorker::build_filesystem_on_demand(ShardId id,
                                                                                            const Configuration& conf) {
    auto info_or = _fetch_shard_info_from_remote(id);
    if (!info_or.ok()) {
        return info_or.status();
    }
    auto fs_or = build_filesystem_from_shard_info(info_or.value(), conf);
    if (!fs_or.ok()) {
        return fs_or.status();
    }

    // Do not return the cache key shared_ptr, so if it not held by anyone else, the fs instance will be removed from the fs cache immediately.
    return fs_or->second;
}

absl::StatusOr<std::pair<std::shared_ptr<std::string>, std::shared_ptr<fslib::FileSystem>>>
StarOSWorker::build_filesystem_from_shard_info(const ShardInfo& info, const Configuration& conf) {
    auto localconf = build_conf_from_shard_info(info);
    if (!localconf.ok()) {
        return localconf.status();
    }
    auto scheme = build_scheme_from_shard_info(info);
    if (!scheme.ok()) {
        return scheme.status();
    }

    return new_shared_filesystem(*scheme, *localconf);
}

bool StarOSWorker::need_enable_cache(const ShardInfo& info) {
    auto cache_info = info.cache_info;
    return cache_info.enable_cache() && !config::starlet_cache_dir.empty();
}

absl::StatusOr<std::string> StarOSWorker::build_scheme_from_shard_info(const ShardInfo& info) {
    if (need_enable_cache(info)) {
        return "cachefs://";
    }

    std::string scheme = "file://";
    switch (info.path_info.fs_info().fs_type()) {
    case staros::FileStoreType::S3:
        scheme = "s3://";
        break;
    case staros::FileStoreType::HDFS:
        scheme = "hdfs://";
        break;
    case staros::FileStoreType::AZBLOB:
        scheme = "azblob://";
        break;
    case staros::FileStoreType::ADLS2:
        scheme = "adls2://";
        break;
    case staros::FileStoreType::GS:
        scheme = "gs://";
        break;
    default:
        return absl::InvalidArgumentError("Unknown shard storage scheme!");
    }
    return scheme;
}

absl::StatusOr<fslib::Configuration> StarOSWorker::build_conf_from_shard_info(const ShardInfo& info) {
    // use the remote fsroot as the default cache identifier
    return info.fslib_conf_from_this(need_enable_cache(info), "");
}

absl::StatusOr<std::pair<std::shared_ptr<std::string>, std::shared_ptr<fslib::FileSystem>>>
StarOSWorker::new_shared_filesystem(std::string_view scheme, const Configuration& conf) {
    std::string cache_key = get_cache_key(scheme, conf);

    // Lookup LRU cache
    auto value_or = find_fs_cache(cache_key);
    if (value_or.ok()) {
        VLOG(9) << "Share filesystem";
        return value_or;
    }

    VLOG(9) << "Create a new filesystem";

    // Create a new instance of FileSystem
    auto fs_or = fslib::FileSystemFactory::new_filesystem(scheme, conf);
    if (!fs_or.ok()) {
        return fs_or.status();
    }
    // turn unique_ptr to shared_ptr
    std::shared_ptr<fslib::FileSystem> fs = std::move(fs_or).value();

    // Put the FileSysatem into LRU cache
    std::unique_lock l(_cache_mtx);
    value_or = find_fs_cache(cache_key);
    if (value_or.ok()) {
        VLOG(9) << "Share filesystem";
        return value_or;
    }
    auto fs_cache_key = insert_fs_cache(cache_key, fs);

    return std::make_pair(std::move(fs_cache_key), std::move(fs));
}

std::string StarOSWorker::get_cache_key(std::string_view scheme, const Configuration& conf) {
    // Take the SHA-256 hash value as the cache key
    SHA256Digest sha256;
    sha256.update(scheme.data(), scheme.size());
    for (const auto& [k, v] : conf) {
        sha256.update(k.data(), k.size());
        sha256.update(v.data(), v.size());
    }
    sha256.digest();
    return sha256.hex();
}

std::shared_ptr<std::string> StarOSWorker::insert_fs_cache(const std::string& key,
                                                           const std::shared_ptr<FileSystem>& fs) {
    std::shared_ptr<std::string> fs_cache_key(new std::string(key), [](std::string* key) {
        if (g_worker) {
            g_worker->erase_fs_cache(*key);
        }
        delete key;
    });

    CacheKey cache_key(key);
    auto value = new CacheValue(fs_cache_key, fs);
    auto handle = _fs_cache->insert(cache_key, value, 1, cache_value_deleter);
    if (handle == nullptr) {
        delete value;
        return nullptr;
    }

    _fs_cache->release(handle);
    return fs_cache_key;
}

void StarOSWorker::erase_fs_cache(const std::string& key) {
    CacheKey cache_key(key);
    _fs_cache->erase(key);
}

std::shared_ptr<fslib::FileSystem> StarOSWorker::lookup_fs_cache(const std::shared_ptr<std::string>& key) {
    if (key == nullptr) {
        return nullptr;
    }
    return lookup_fs_cache(*key);
}

std::shared_ptr<fslib::FileSystem> StarOSWorker::lookup_fs_cache(const std::string& key) {
    auto value_or = find_fs_cache(key);
    if (!value_or.ok()) {
        return nullptr;
    }

    return value_or->second;
}

absl::StatusOr<std::pair<std::shared_ptr<std::string>, std::shared_ptr<fslib::FileSystem>>> StarOSWorker::find_fs_cache(
        const std::string& key) {
    if (key.empty()) {
        return absl::InvalidArgumentError("key is empty");
    }

    CacheKey cache_key(key);
    auto handle = _fs_cache->lookup(cache_key);
    if (handle == nullptr) {
        return absl::NotFoundError(key + " not found");
    }

    DeferOp op([this, handle] { _fs_cache->release(handle); });

    auto value = static_cast<CacheValue*>(_fs_cache->value(handle));

    auto fs_cache_ttl_sec = config::starlet_filesystem_instance_cache_ttl_sec;
    if (fs_cache_ttl_sec >= 0) {
        int32_t duration = MonotonicSeconds() - value->created_time_sec;
        if (duration > fs_cache_ttl_sec) {
            return absl::NotFoundError(key + " is expired");
        }
    }

    // The value->key may be expired in a very short critical moment.
    // At that moment, the value->key is not referenced by anyone but it's shared_ptr deleter haven't be executed,
    // so the item haven't be removed from cache yet.
    // In this situation, this function will return a null key and a valid fs instance.
    // So the caller cannot assume the returned key always valid.
    return std::make_pair(value->key.lock(), value->fs);
}

Status to_status(const absl::Status& absl_status) {
    switch (absl_status.code()) {
    case absl::StatusCode::kOk:
        return Status::OK();
    case absl::StatusCode::kAlreadyExists:
        return Status::AlreadyExist(fmt::format("starlet err {}", absl_status.message()));
    case absl::StatusCode::kOutOfRange:
        return Status::InvalidArgument(fmt::format("starlet err {}", absl_status.message()));
    case absl::StatusCode::kInvalidArgument:
        return Status::InvalidArgument(fmt::format("starlet err {}", absl_status.message()));
    case absl::StatusCode::kNotFound:
        return Status::NotFound(fmt::format("starlet err {}", absl_status.message()));
    case absl::StatusCode::kResourceExhausted:
        return Status::ResourceBusy(fmt::format("starlet err {}", absl_status.message()));
    default:
        return Status::InternalError(fmt::format("starlet err {}", absl_status.message()));
    }
}

void init_staros_worker(const std::shared_ptr<starcache::StarCache>& star_cache) {
    if (g_starlet.get() != nullptr) {
        return;
    }

    if (star_cache) {
        (void)fslib::set_star_cache(star_cache);
    }

    // skip staros reinit aws sdk
    staros::starlet::fslib::skip_aws_init_api = true;

    staros::starlet::common::GFlagsUtils::UpdateFlagValue("cachemgr_threadpool_size",
                                                          std::to_string(config::starlet_cache_thread_num));
    staros::starlet::common::GFlagsUtils::UpdateFlagValue("fs_stream_buffer_size_bytes",
                                                          std::to_string(config::starlet_fs_stream_buffer_size_bytes));
    staros::starlet::common::GFlagsUtils::UpdateFlagValue("fs_enable_buffer_prefetch",
                                                          std::to_string(config::starlet_fs_read_prefetch_enable));
    staros::starlet::common::GFlagsUtils::UpdateFlagValue(
            "fs_buffer_prefetch_threadpool_size", std::to_string(config::starlet_fs_read_prefetch_threadpool_size));

    FLAGS_fslib_s3_virtual_address_domainlist = config::starlet_s3_virtual_address_domainlist;
    // use the same configuration as the external query
    FLAGS_fslib_s3client_max_connections = config::object_storage_max_connection;
    FLAGS_fslib_s3client_max_items = config::starlet_s3_client_max_cache_capacity;
    FLAGS_fslib_s3client_max_instance_per_item = config::starlet_s3_client_num_instances_per_cache;
    FLAGS_fslib_s3client_nonread_max_retries = config::starlet_fslib_s3client_nonread_max_retries;
    FLAGS_fslib_s3client_nonread_retry_scale_factor = config::starlet_fslib_s3client_nonread_retry_scale_factor;
    FLAGS_fslib_s3client_connect_timeout_ms = config::starlet_fslib_s3client_connect_timeout_ms;
    FLAGS_fslib_s3client_use_list_objects_v1 = config::s3_use_list_objects_v1;
    if (config::object_storage_request_timeout_ms >= 0) {
        FLAGS_fslib_s3client_request_timeout_ms = static_cast<int32_t>(config::object_storage_request_timeout_ms);
    }
    fslib::FLAGS_delete_files_max_key_in_batch = config::starlet_delete_files_max_key_in_batch;

    fslib::FLAGS_use_star_cache = config::starlet_use_star_cache;
    fslib::FLAGS_star_cache_async_init = config::starlet_star_cache_async_init;
    fslib::FLAGS_star_cache_mem_size_percent = config::starlet_star_cache_mem_size_percent;
    fslib::FLAGS_star_cache_mem_size_bytes = config::starlet_star_cache_mem_size_bytes;
    fslib::FLAGS_star_cache_disk_size_percent = config::starlet_star_cache_disk_size_percent;
    fslib::FLAGS_star_cache_disk_size_bytes = config::starlet_star_cache_disk_size_bytes;
    fslib::FLAGS_star_cache_block_size_bytes = config::starlet_star_cache_block_size_bytes;

    staros::starlet::StarletConfig starlet_config;
    starlet_config.rpc_port = config::starlet_port;
    g_worker = std::make_shared<StarOSWorker>();
    g_starlet = std::make_unique<staros::starlet::Starlet>(g_worker);
    g_starlet->init(starlet_config);
    g_starlet->start();
}

void shutdown_staros_worker() {
    g_starlet->stop();
    g_starlet.reset();
    g_worker = nullptr;

    LOG(INFO) << "Executing starlet shutdown hooks ...";
    staros::starlet::common::ShutdownHook::shutdown();
}

// must keep each config the same
void update_staros_starcache() {
    if (fslib::FLAGS_use_star_cache != config::starlet_use_star_cache) {
        fslib::FLAGS_use_star_cache = config::starlet_use_star_cache;
        (void)fslib::star_cache_init(fslib::FLAGS_use_star_cache);
    }

    if (fslib::FLAGS_star_cache_mem_size_percent != config::starlet_star_cache_mem_size_percent) {
        fslib::FLAGS_star_cache_mem_size_percent = config::starlet_star_cache_mem_size_percent;
        (void)fslib::star_cache_update_memory_quota_percent(fslib::FLAGS_star_cache_mem_size_percent);
    }

    if (fslib::FLAGS_star_cache_mem_size_bytes != config::starlet_star_cache_mem_size_bytes) {
        fslib::FLAGS_star_cache_mem_size_bytes = config::starlet_star_cache_mem_size_bytes;
        (void)fslib::star_cache_update_memory_quota_bytes(fslib::FLAGS_star_cache_mem_size_bytes);
    }
}

} // namespace starrocks
#endif // USE_STAROS
