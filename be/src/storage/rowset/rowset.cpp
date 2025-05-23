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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/rowset.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <unistd.h>

#include <memory>
#include <set>

#include "fmt/format.h"
#include "fs/fs_util.h"
#include "gutil/strings/substitute.h"
#include "rowset_options.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "segment_options.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/delete_predicates.h"
#include "storage/empty_iterator.h"
#include "storage/index/index_descriptor.h"
#include "storage/merge_iterator.h"
#include "storage/projection_iterator.h"
#include "storage/rowset/metadata_cache.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/storage_engine.h"
#include "storage/tablet_index.h"
#include "storage/tablet_manager.h"
#include "storage/tablet_meta_manager.h"
#include "storage/union_iterator.h"
#include "storage/update_manager.h"
#include "storage/utils.h"
#include "util/defer_op.h"
#include "util/time.h"

namespace starrocks {

Rowset::Rowset(const TabletSchemaCSPtr& schema, std::string rowset_path, RowsetMetaSharedPtr rowset_meta)
        : _schema(schema),
          _rowset_path(std::move(rowset_path)),
          _rowset_meta(std::move(rowset_meta)),
          _refs_by_reader(0) {
    _schema = _rowset_meta->tablet_schema() ? _rowset_meta->tablet_schema() : schema;
    _keys_type = _schema->keys_type();
    MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->rowset_metadata_mem_tracker(), _mem_usage());
}

Rowset::~Rowset() {
#ifndef BE_TEST
    if (_keys_type != PRIMARY_KEYS) {
        // ONLY support non-pk table now.
        // evict rowset before destroy, in case this rowset no close yet.
        MetadataCache::instance()->evict_rowset(this);
    }
#endif
    MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->rowset_metadata_mem_tracker(), _mem_usage());
}

Status Rowset::load() {
    // before lock, if rowset state is ROWSET_UNLOADING, maybe it is doing do_close in release
    std::lock_guard<std::mutex> load_lock(_lock);
    // if the state is ROWSET_UNLOADING it means close() is called
    // and the rowset is already loaded, and the resource is not closed yet.
    if (_rowset_state_machine.rowset_state() == ROWSET_LOADED) {
        warmup_lrucache();
        return Status::OK();
    }
    // after lock, if rowset state is ROWSET_UNLOADING, it is ok to return
    if (_rowset_state_machine.rowset_state() == ROWSET_UNLOADED) {
        // first do load, then change the state
        RETURN_IF_ERROR(do_load());
        RETURN_IF_ERROR(_rowset_state_machine.on_load());
    }
    VLOG(2) << "rowset is loaded. rowset version:" << start_version() << "-" << end_version()
            << ", state from ROWSET_UNLOADED to ROWSET_LOADED. tabletid:" << _rowset_meta->tablet_id();
    return Status::OK();
}

void Rowset::make_visible(Version version) {
    _rowset_meta->set_version(version);
    _rowset_meta->set_rowset_state(VISIBLE);
    // update create time to the visible time,
    // it's used to skip recently published version during compaction
    _rowset_meta->set_creation_time(UnixSeconds());

    if (_rowset_meta->has_delete_predicate()) {
        _rowset_meta->mutable_delete_predicate()->set_version(version.first);
        return;
    }
    make_visible_extra(version);
}

void Rowset::make_commit(int64_t version, uint32_t rowset_seg_id) {
    _rowset_meta->set_rowset_seg_id(rowset_seg_id);
    Version v(version, version);
    _rowset_meta->set_version(v);
    _rowset_meta->set_rowset_state(VISIBLE);
    // update create time to the visible time,
    // it's used to skip recently published version during compaction
    _rowset_meta->set_creation_time(UnixSeconds());

    if (_rowset_meta->has_delete_predicate()) {
        _rowset_meta->mutable_delete_predicate()->set_version(version);
        return;
    }
    make_visible_extra(v);
}

/**
 * Checks if all files associated with this rowset exist on disk.
 * 
 * This method verifies the existence of: All segment files
 * 
 * If any file is missing, it logs a warning message with the expected path
 * and tablet ID, then returns false.
 * 
 * @return true if all associated files exist, false otherwise
 */
bool Rowset::check_file_existence() {
    for (int i = 0; i < num_segments(); ++i) {
        std::string seg_path = segment_file_path(_rowset_path, rowset_id(), i);
        if (!fs::path_exist(seg_path)) {
            LOG(WARNING) << "Segment file does not exist. Expected path: " << seg_path
                         << ". This might occur if the file was deleted or not generated correctly. "
                         << "Tablet ID: " << _rowset_meta->tablet_id();
            return false;
        }
    }
    // All files were found successfully.
    return true;
}

void Rowset::make_commit(int64_t version, uint32_t rowset_seg_id, uint32_t max_compact_input_rowset_id) {
    _rowset_meta->set_max_compact_input_rowset_id(max_compact_input_rowset_id);
    make_commit(version, rowset_seg_id);
}

std::string Rowset::segment_file_path(const std::string& dir, const RowsetId& rowset_id, int segment_id) {
    return strings::Substitute("$0/$1_$2.dat", dir, rowset_id.to_string(), segment_id);
}

std::string Rowset::segment_temp_file_path(const std::string& dir, const RowsetId& rowset_id, int segment_id) {
    return strings::Substitute("$0/$1_$2.dat.tmp", dir, rowset_id.to_string(), segment_id);
}

std::string Rowset::segment_del_file_path(const std::string& dir, const RowsetId& rowset_id, int segment_id) {
    return strings::Substitute("$0/$1_$2.del", dir, rowset_id.to_string(), segment_id);
}

std::string Rowset::segment_upt_file_path(const std::string& dir, const RowsetId& rowset_id, int segment_id) {
    return strings::Substitute("$0/$1_$2.upt", dir, rowset_id.to_string(), segment_id);
}

std::string Rowset::delta_column_group_path(const std::string& dir, const RowsetId& rowset_id, int segment_id,
                                            int64_t version, int idx) {
    return strings::Substitute("$0/$1_$2_$3_$4.cols", dir, rowset_id.to_string(), segment_id, version, idx);
}

Status Rowset::init() {
    return Status::OK();
}

StatusOr<std::shared_ptr<Segment>> Rowset::_load_segment(int32_t idx, const TabletSchemaCSPtr& schema,
                                                         std::shared_ptr<FileSystem>& fs,
                                                         const FooterPointerPB* partial_rowset_footer,
                                                         size_t* footer_size_hint) {
    std::string seg_path = segment_file_path(_rowset_path, rowset_id(), idx);
    FileInfo seg_info{.path = seg_path, .encryption_meta = rowset_meta()->get_segment_encryption_meta(idx)};
    auto res = Segment::open(fs, seg_info, idx, schema, footer_size_hint, partial_rowset_footer);
    if (!res.ok()) {
        auto st = res.status().clone_and_prepend(fmt::format(
                "Load segment failed tablet:{} rowset:{} rssid:{} seg:{} path:{}", _rowset_meta->tablet_id(),
                rowset_id().to_string(), _rowset_meta->get_rowset_seg_id(), idx, seg_path));
        LOG(WARNING) << st.message();
        return st;
    }
    return res;
}

// use partial_rowset_footer to indicate the segment footer position and size
// if partial_rowset_footer is nullptr, the segment_footer is at the end of the segment_file
Status Rowset::do_load() {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_rowset_path));
    _segments.clear();
    size_t footer_size_hint = 16 * 1024;
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        auto res = _load_segment(seg_id, _schema, fs, rowset_meta()->partial_rowset_footer(seg_id), &footer_size_hint);
        if (!res.ok()) {
            _segments.clear();
            return res.status();
        }
        _segments.push_back(std::move(res).value());
    }
#ifndef BE_TEST
    if (config::metadata_cache_memory_limit_percent > 0 && _keys_type != PRIMARY_KEYS) {
        // Add rowset to lru metadata cache for memory control.
        // ONLY support non-pk table now.
        MetadataCache::instance()->cache_rowset(this);
    }
#endif
    return Status::OK();
}

void Rowset::warmup_lrucache() {
#ifndef BE_TEST
    if (config::metadata_cache_memory_limit_percent > 0 && _keys_type != PRIMARY_KEYS) {
        // Move this item to newest item in lru cache.
        // ONLY support non-pk table now.
        MetadataCache::instance()->refresh_rowset(this);
    }
#endif
}

// this function is only used for partial update so far
// make sure segment_footer is in the end of segment_file before call this function
Status Rowset::reload() {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_rowset_path));
    _segments.clear();
    size_t footer_size_hint = 16 * 1024;
    for (int seg_id = 0; seg_id < num_segments(); ++seg_id) {
        auto res = _load_segment(seg_id, _schema, fs, nullptr, &footer_size_hint);
        if (!res.ok()) {
            _segments.clear();
            return res.status();
        }
        _segments.push_back(std::move(res).value());
    }
    return Status::OK();
}

Status Rowset::reload_segment(int32_t segment_id) {
    DCHECK(_segments.size() > segment_id);
    if (_segments.size() <= segment_id) {
        LOG(WARNING) << "Error segment id: " << segment_id;
        return Status::InternalError("Error segment id");
    }
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_rowset_path));
    size_t footer_size_hint = 16 * 1024;
    auto res = _load_segment(segment_id, _schema, fs, nullptr, &footer_size_hint);
    if (!res.ok()) {
        return res.status();
    }
    _segments[segment_id] = std::move(res).value();
    return Status::OK();
}

Status Rowset::reload_segment_with_schema(int32_t segment_id, TabletSchemaCSPtr& schema) {
    DCHECK(_segments.size() > segment_id);
    if (_segments.size() <= segment_id) {
        LOG(WARNING) << "Error segment id: " << segment_id;
        return Status::InternalError("Error segment id");
    }
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_rowset_path));
    size_t footer_size_hint = 16 * 1024;
    auto res = _load_segment(segment_id, schema, fs, nullptr, &footer_size_hint);
    if (!res.ok()) {
        _segments.clear();
        return res.status();
    }
    _segments[segment_id] = std::move(res).value();
    return Status::OK();
}

StatusOr<int64_t> Rowset::total_segment_data_size() {
    int64_t res = 0;
    for (auto& seg : _segments) {
        if (seg != nullptr) {
            ASSIGN_OR_RETURN(auto sz, seg->get_data_size());
            res += sz;
        }
    }
    return res;
}

StatusOr<int64_t> Rowset::estimate_compaction_segment_iterator_num() {
    if (num_segments() == 0) {
        return 0;
    }

    int64_t segment_num = 0;
    acquire();
    DeferOp defer([this]() { release(); });
    RETURN_IF_ERROR(load());
    for (auto& seg_ptr : segments()) {
        if (seg_ptr->num_rows() == 0) {
            continue;
        }
        // When creating segment iterators for compaction, we don't provide rowid_range_option and pred_tree_for_zone_map,
        // So here we don't need to consider the following two situation:
        //
        // if (options.rowid_range_option != nullptr) {
        //    seg_options.rowid_range_option = options.rowid_range_option->get_segment_rowid_range(this, seg_ptr.get());
        //    if (seg_options.rowid_range_option == nullptr) {
        //        continue;
        //    }
        // }
        //    auto res = seg_ptr->new_iterator(segment_schema, seg_options);
        //    if (res.status().is_end_of_file()) {
        //     continue;
        //    }

        segment_num++;
    }

    if (segment_num == 0) {
        return 0;
    } else if (rowset_meta()->is_segments_overlapping()) {
        return segment_num;
    } else {
        return 1;
    }
}

Status Rowset::remove() {
    VLOG(2) << "Removing files in rowset id=" << unique_id() << " version=" << start_version() << "-" << end_version()
            << " tablet_id=" << _rowset_meta->tablet_id();
    Status result;
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_rowset_path));
    auto merge_status = [&](const Status& st) {
        if (result.ok() && !st.ok() && !st.is_not_found()) result = st;
    };

    for (int i = 0, sz = num_segments(); i < sz; ++i) {
        std::string path = segment_file_path(_rowset_path, rowset_id(), i);
        auto st = fs->delete_file(path);
        LOG_IF(WARNING, !st.ok()) << "Fail to delete " << path << ": " << st;
        merge_status(st);

        // delete index
        for (const auto& index : *(_schema->indexes())) {
            if (index.index_type() == IndexType::GIN) {
                std::string inverted_index_path = IndexDescriptor::inverted_index_file_path(
                        _rowset_path, rowset_id().to_string(), i, index.index_id());
                auto ist = fs->delete_dir_recursive(inverted_index_path);
                LOG_IF(WARNING, !ist.ok()) << "Fail to delete vector_index_path " << inverted_index_path << ": " << ist;
                merge_status(ist);
            } else if (index.index_type() == IndexType::VECTOR) {
                std::string vector_index_path = IndexDescriptor::vector_index_file_path(
                        _rowset_path, rowset_id().to_string(), i, index.index_id());
                auto vst = fs->delete_file(vector_index_path);
                LOG_IF(WARNING, !vst.ok()) << "Fail to delete vector_index_path " << vector_index_path << ": " << vst;
                merge_status(vst);
            }
        }
    }
    for (int i = 0, sz = num_delete_files(); i < sz; ++i) {
        std::string path = segment_del_file_path(_rowset_path, rowset_id(), i);
        auto st = fs->delete_file(path);
        LOG_IF(WARNING, !st.ok()) << "Fail to delete " << path << ": " << st;
        merge_status(st);
    }
    for (int i = 0, sz = num_update_files(); i < sz; ++i) {
        std::string path = segment_upt_file_path(_rowset_path, rowset_id(), i);
        auto st = fs->delete_file(path);
        LOG_IF(WARNING, !st.ok()) << "Fail to delete " << path << ": " << st;
        merge_status(st);
    }
    return result;
}

Status Rowset::remove_delta_column_group() {
    std::filesystem::path schema_hash_path(_rowset_path);
    std::filesystem::path data_dir_path = schema_hash_path.parent_path().parent_path().parent_path().parent_path();
    std::string data_dir_string = data_dir_path.string();
    DataDir* data_dir = StorageEngine::instance()->get_store(data_dir_string);
    if (data_dir == nullptr) {
        LOG(ERROR) << "DataDir not found! rowset_path: " << _rowset_path << ", dir_path: " << data_dir_string;
        return Status::OK();
    }
    return remove_delta_column_group(data_dir->get_meta());
}

Status Rowset::remove_delta_column_group(KVStore* kvstore) {
    ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(_rowset_path));
    return _remove_delta_column_group_files(fs, kvstore);
}

Status Rowset::_remove_delta_column_group_files(const std::shared_ptr<FileSystem>& fs, KVStore* kvstore) {
    if (num_segments() > 0) {
        // 1. remove dcg files
        for (int i = 0; i < num_segments(); i++) {
            DeltaColumnGroupList list;
            if (_keys_type == PRIMARY_KEYS) {
                RETURN_IF_ERROR(TabletMetaManager::scan_delta_column_group(kvstore, _rowset_meta->tablet_id(),
                                                                           _rowset_meta->get_rowset_seg_id() + i, 0,
                                                                           INT64_MAX, &list));
            } else {
                RETURN_IF_ERROR(TabletMetaManager::scan_delta_column_group(
                        kvstore, _rowset_meta->tablet_id(), _rowset_meta->rowset_id(), i, 0, INT64_MAX, &list));
            }

            for (const auto& dcg : list) {
                auto column_files = dcg->column_files(_rowset_path);
                for (const auto& column_file : column_files) {
                    auto st = fs->delete_file(column_file);
                    if (st.ok() || st.is_not_found()) {
                        VLOG(2) << "Deleting delta column group's file: " << dcg->debug_string() << " st: " << st;
                    } else {
                        return st;
                    }
                }
            }
        }
        // 2. remove dcg from rocksdb
        if (_keys_type == PRIMARY_KEYS) {
            RETURN_IF_ERROR(TabletMetaManager::delete_delta_column_group(
                    kvstore, _rowset_meta->tablet_id(), _rowset_meta->get_rowset_seg_id(), num_segments()));
        } else {
            RETURN_IF_ERROR(TabletMetaManager::delete_delta_column_group(kvstore, _rowset_meta->tablet_id(),
                                                                         _rowset_meta->rowset_id(), num_segments()));
        }
    }
    return Status::OK();
}

Status Rowset::link_files_to(KVStore* kvstore, const std::string& dir, RowsetId new_rowset_id, int64_t version) {
    for (int i = 0; i < num_segments(); ++i) {
        std::string dst_link_path = segment_file_path(dir, new_rowset_id, i);
        std::string src_file_path = segment_file_path(_rowset_path, rowset_id(), i);
        if (link(src_file_path.c_str(), dst_link_path.c_str()) != 0) {
            PLOG(WARNING) << "Fail to link " << src_file_path << " to " << dst_link_path;
            return Status::RuntimeError(
                    strings::Substitute("Fail to link segment data file from $0 to $1", src_file_path, dst_link_path));
        }

        // link inverted files
        if (!_schema->indexes()->empty()) {
            int segment_n = i;
            const auto& indexes = *_schema->indexes();
            for (const auto& index : indexes) {
                if (index.index_type() == GIN) {
                    std::string dst_inverted_link_path = IndexDescriptor::inverted_index_file_path(
                            dir, new_rowset_id.to_string(), segment_n, index.index_id());
                    std::string src_inverted_file_path = IndexDescriptor::inverted_index_file_path(
                            _rowset_path, rowset_id().to_string(), segment_n, index.index_id());

                    RETURN_IF_ERROR(fs::create_directories(dst_inverted_link_path));
                    std::set<std::string> files;
                    RETURN_IF_ERROR(fs::list_dirs_files(src_inverted_file_path, nullptr, &files));
                    for (const auto& file : files) {
                        auto src_absolute_path = fmt::format("{}/{}", src_inverted_file_path, file);
                        auto dst_absolute_path = fmt::format("{}/{}", dst_inverted_link_path, file);

                        if (link(src_absolute_path.c_str(), dst_absolute_path.c_str()) != 0) {
                            PLOG(WARNING) << "Fail to link " << src_absolute_path << " to " << dst_absolute_path;
                            return Status::RuntimeError(strings::Substitute("Fail to link index gin file from $0 to $1",
                                                                            src_absolute_path, dst_absolute_path));
                        }
                    }
                } else if (index.index_type() == VECTOR) {
                    std::string dst_index_link_path = IndexDescriptor::vector_index_file_path(
                            dir, new_rowset_id.to_string(), segment_n, index.index_id());
                    std::string src_index_file_path = IndexDescriptor::vector_index_file_path(
                            _rowset_path, rowset_id().to_string(), segment_n, index.index_id());
                    if (link(src_index_file_path.c_str(), dst_index_link_path.c_str()) != 0) {
                        PLOG(WARNING) << "Fail to link " << src_index_file_path << " to " << dst_index_link_path;
                        return Status::RuntimeError("Fail to link index data file");
                    }
                }
            }
        }
    }
    for (int i = 0; i < num_delete_files(); ++i) {
        std::string src_file_path = segment_del_file_path(_rowset_path, rowset_id(), i);
        std::string dst_link_path = segment_del_file_path(dir, new_rowset_id, i);
        if (link(src_file_path.c_str(), dst_link_path.c_str()) != 0) {
            PLOG(WARNING) << "Fail to link " << src_file_path << " to " << dst_link_path;
            return Status::RuntimeError("Fail to link segment delete file");
        }
    }
    for (int i = 0; i < num_update_files(); ++i) {
        std::string src_file_path = segment_upt_file_path(_rowset_path, rowset_id(), i);
        std::string dst_link_path = segment_upt_file_path(dir, new_rowset_id, i);
        if (link(src_file_path.c_str(), dst_link_path.c_str()) != 0) {
            PLOG(WARNING) << "Fail to link " << src_file_path << " to " << dst_link_path;
            return Status::RuntimeError(
                    fmt::format("Fail to link segment update file, src: {}, dst {}", src_file_path, dst_link_path));
        } else {
            VLOG(2) << "success to link " << src_file_path << " to " << dst_link_path;
        }
    }
    RETURN_IF_ERROR(_link_delta_column_group_files(kvstore, dir, version));
    return Status::OK();
}

Status Rowset::_link_delta_column_group_files(KVStore* kvstore, const std::string& dir, int64_t version) {
    if (num_segments() > 0 && kvstore != nullptr && _rowset_path != dir) {
        // link dcg files
        for (int i = 0; i < num_segments(); i++) {
            DeltaColumnGroupList list;

            if (_keys_type == PRIMARY_KEYS) {
                RETURN_IF_ERROR(TabletMetaManager::scan_delta_column_group(
                        kvstore, _rowset_meta->tablet_id(), _rowset_meta->get_rowset_seg_id() + i, 0, version, &list));
            } else {
                RETURN_IF_ERROR(TabletMetaManager::scan_delta_column_group(
                        kvstore, _rowset_meta->tablet_id(), _rowset_meta->rowset_id(), i, 0, INT64_MAX, &list));
            }

            for (const auto& dcg : list) {
                std::vector<std::string> src_file_paths = dcg->column_files(_rowset_path);
                std::vector<std::string> dst_link_paths = dcg->column_files(dir);

                for (int j = 0; j < src_file_paths.size(); ++j) {
                    const std::string& src_file_path = src_file_paths[j];
                    const std::string& dst_link_path = dst_link_paths[j];

                    if (link(src_file_path.c_str(), dst_link_path.c_str()) != 0) {
                        LOG(WARNING) << "Fail to link " << src_file_path << " to " << dst_link_path;
                        return Status::RuntimeError(fmt::format("Fail to link segment cols file, src: {}, dst {}",
                                                                src_file_path, dst_link_path));
                    } else {
                        VLOG(2) << "success to link " << src_file_path << " to " << dst_link_path;
                    }
                }
            }
        }
    }
    return Status::OK();
}

Status Rowset::copy_files_to(KVStore* kvstore, const std::string& dir) {
    for (int i = 0; i < num_segments(); ++i) {
        std::string dst_path = segment_file_path(dir, rowset_id(), i);
        if (fs::path_exist(dst_path)) {
            LOG(WARNING) << "Path already exist: " << dst_path;
            return Status::AlreadyExist(fmt::format("Path already exist: {}", dst_path));
        }
        std::string src_path = segment_file_path(_rowset_path, rowset_id(), i);
        if (!fs::copy_file(src_path, dst_path).ok()) {
            LOG(WARNING) << "Error to copy file. src:" << src_path << ", dst:" << dst_path
                         << ", errno=" << std::strerror(Errno::no());
            return Status::IOError(fmt::format("Error to copy file. src: {}, dst: {}, error:{} ", src_path, dst_path,
                                               std::strerror(Errno::no())));
        }
        // copy index
        const auto& indexes = *_schema->indexes();
        if (!indexes.empty()) {
            for (const auto& index : indexes) {
                if (index.index_type() == IndexType::GIN) {
                    std::string dst_index_path = IndexDescriptor::inverted_index_file_path(dir, rowset_id().to_string(),
                                                                                           i, index.index_id());
                    if (fs::path_exist(dst_index_path)) {
                        LOG(WARNING) << "Index path already exist: " << dst_path;
                        return Status::AlreadyExist(fmt::format("Index path already exist: {}", dst_path));
                    }

                    std::string src_index_path = IndexDescriptor::inverted_index_file_path(
                            _rowset_path, rowset_id().to_string(), i, index.index_id());

                    std::set<std::string> files;
                    RETURN_IF_ERROR(fs::list_dirs_files(src_index_path, nullptr, &files));
                    for (const auto& file : files) {
                        auto src_absolute_path = fmt::format("{}/{}", src_index_path, file);
                        auto dst_absolute_path =
                                fmt::format("{}/{}_{}_{}_{}", dir, rowset_id().to_string(), i, index.index_id(), file);

                        if (!fs::copy_file(src_absolute_path, dst_absolute_path).ok()) {
                            LOG(WARNING) << "Error to copy index. src:" << src_absolute_path
                                         << ", dst:" << dst_absolute_path << ", errno=" << std::strerror(Errno::no());
                            return Status::IOError(fmt::format("Error to copy file. src: {}, dst: {}, error:{} ",
                                                               src_absolute_path, dst_absolute_path,
                                                               std::strerror(Errno::no())));
                        }
                    }
                }
            }
        }
    }
    for (int i = 0; i < num_delete_files(); ++i) {
        std::string src_path = segment_del_file_path(_rowset_path, rowset_id(), i);
        if (fs::path_exist(src_path)) {
            std::string dst_path = segment_del_file_path(dir, rowset_id(), i);
            if (fs::path_exist(dst_path)) {
                LOG(WARNING) << "Path already exist: " << dst_path;
                return Status::AlreadyExist(fmt::format("Path already exist: {}", dst_path));
            }
            if (!fs::copy_file(src_path, dst_path).ok()) {
                LOG(WARNING) << "Error to copy file. src:" << src_path << ", dst:" << dst_path
                             << ", errno=" << std::strerror(Errno::no());
                return Status::IOError(fmt::format("Error to copy file. src: {}, dst: {}, error:{} ", src_path,
                                                   dst_path, std::strerror(Errno::no())));
            }
        }
    }
    for (int i = 0; i < num_update_files(); ++i) {
        std::string src_path = segment_upt_file_path(_rowset_path, rowset_id(), i);
        if (fs::path_exist(src_path)) {
            std::string dst_path = segment_upt_file_path(dir, rowset_id(), i);
            if (fs::path_exist(dst_path)) {
                LOG(WARNING) << "Path already exist: " << dst_path;
                return Status::AlreadyExist(fmt::format("Path already exist: {}", dst_path));
            }
            if (!fs::copy_file(src_path, dst_path).ok()) {
                LOG(WARNING) << "Error to copy file. src:" << src_path << ", dst:" << dst_path
                             << ", errno=" << std::strerror(Errno::no());
                return Status::IOError(fmt::format("Error to copy file. src: {}, dst: {}, error:{} ", src_path,
                                                   dst_path, std::strerror(Errno::no())));
            }
        }
    }
    RETURN_IF_ERROR(_copy_delta_column_group_files(kvstore, dir, INT64_MAX));
    return Status::OK();
}

Status Rowset::_copy_delta_column_group_files(KVStore* kvstore, const std::string& dir, int64_t version) {
    if (num_segments() > 0 && kvstore != nullptr && _rowset_path != dir) {
        // link dcg files
        for (int i = 0; i < num_segments(); i++) {
            DeltaColumnGroupList list;

            if (_keys_type == PRIMARY_KEYS) {
                RETURN_IF_ERROR(TabletMetaManager::scan_delta_column_group(
                        kvstore, _rowset_meta->tablet_id(), _rowset_meta->get_rowset_seg_id() + i, 0, version, &list));
            } else {
                RETURN_IF_ERROR(TabletMetaManager::scan_delta_column_group(
                        kvstore, _rowset_meta->tablet_id(), _rowset_meta->rowset_id(), i, 0, INT64_MAX, &list));
            }

            for (const auto& dcg : list) {
                std::vector<std::string> src_file_paths = dcg->column_files(_rowset_path);
                std::vector<std::string> dst_copy_paths = dcg->column_files(dir);

                for (int j = 0; j < src_file_paths.size(); ++j) {
                    const std::string& src_file_path = src_file_paths[j];
                    const std::string& dst_copy_path = dst_copy_paths[j];

                    if (fs::path_exist(dst_copy_path)) {
                        LOG(WARNING) << "Path already exist: " << dst_copy_path;
                        return Status::AlreadyExist(fmt::format("Path already exist: {}", dst_copy_path));
                    }

                    if (!fs::copy_file(src_file_path.c_str(), dst_copy_path.c_str()).ok()) {
                        LOG(WARNING) << "Fail to copy " << src_file_path << " to " << dst_copy_path;
                        return Status::RuntimeError(fmt::format("Fail to copy segment cols file, src: {}, dst {}",
                                                                src_file_path, dst_copy_path));
                    } else {
                        VLOG(2) << "success to copy " << src_file_path << " to " << dst_copy_path;
                    }
                }
            }
        }
    }
    return Status::OK();
}

void Rowset::do_close() {
    _segments.clear();
}

size_t Rowset::segment_memory_usage() {
    size_t total = 0;
    for (const auto& segment : _segments) {
        total += segment->mem_usage();
    }
    return total;
}

class SegmentIteratorWrapper : public ChunkIterator {
public:
    SegmentIteratorWrapper(std::shared_ptr<Rowset> rowset, ChunkIteratorPtr iter)
            : ChunkIterator(iter->schema(), iter->chunk_size()), _guard(std::move(rowset)), _iter(std::move(iter)) {}

    void close() override {
        _iter->close();
        _iter.reset();
    }

    Status init_encoded_schema(ColumnIdToGlobalDictMap& dict_maps) override {
        RETURN_IF_ERROR(ChunkIterator::init_encoded_schema(dict_maps));
        return _iter->init_encoded_schema(dict_maps);
    }

    Status init_output_schema(const std::unordered_set<uint32_t>& unused_output_column_ids) override {
        RETURN_IF_ERROR(ChunkIterator::init_output_schema(unused_output_column_ids));
        return _iter->init_output_schema(unused_output_column_ids);
    }

protected:
    Status do_get_next(Chunk* chunk) override { return _iter->get_next(chunk); }
    Status do_get_next(Chunk* chunk, vector<uint32_t>* rowid) override { return _iter->get_next(chunk, rowid); }

private:
    RowsetReleaseGuard _guard;
    ChunkIteratorPtr _iter;
};

StatusOr<ChunkIteratorPtr> Rowset::new_iterator(const Schema& schema, const RowsetReadOptions& options) {
    std::vector<ChunkIteratorPtr> seg_iters;
    RETURN_IF_ERROR(get_segment_iterators(schema, options, &seg_iters));
    if (seg_iters.empty()) {
        return new_empty_iterator(schema, options.chunk_size);
    } else if (options.sorted) {
        return new_heap_merge_iterator(seg_iters);
    } else {
        return new_union_iterator(std::move(seg_iters));
    }
}

Status Rowset::get_segment_iterators(const Schema& schema, const RowsetReadOptions& options,
                                     std::vector<ChunkIteratorPtr>* segment_iterators) {
    RowsetReleaseGuard guard(shared_from_this());
    RETURN_IF_ERROR(load());

    SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(_rowset_path));
    seg_options.stats = options.stats;
    seg_options.ranges = options.ranges;
    seg_options.pred_tree = options.pred_tree;
    seg_options.runtime_filter_preds = options.runtime_filter_preds;
    seg_options.pred_tree_for_zone_map = options.pred_tree_for_zone_map;
    seg_options.use_page_cache = options.use_page_cache;
    seg_options.profile = options.profile;
    seg_options.reader_type = options.reader_type;
    seg_options.chunk_size = options.chunk_size;
    seg_options.global_dictmaps = options.global_dictmaps;
    seg_options.unused_output_column_ids = options.unused_output_column_ids;
    seg_options.runtime_range_pruner = options.runtime_range_pruner;
    seg_options.column_access_paths = options.column_access_paths;
    seg_options.tablet_schema = options.tablet_schema;
    seg_options.use_vector_index = options.use_vector_index;
    seg_options.vector_search_option = options.vector_search_option;
    seg_options.sample_options = options.sample_options;
    seg_options.enable_join_runtime_filter_pushdown = options.enable_join_runtime_filter_pushdown;

    if (options.delete_predicates != nullptr) {
        seg_options.delete_predicates = options.delete_predicates->get_predicates(end_version());
    }
    if (options.is_primary_keys) {
        seg_options.is_primary_keys = true;
        seg_options.rowset_id = rowset_meta()->get_rowset_seg_id();
        seg_options.version = options.version;
        seg_options.delvec_loader = std::make_shared<LocalDelvecLoader>(options.meta);
    }
    seg_options.rowset_path = _rowset_path;
    seg_options.tablet_id = rowset_meta()->tablet_id();
    seg_options.rowsetid = rowset_meta()->rowset_id();
    seg_options.dcg_loader = std::make_shared<LocalDeltaColumnGroupLoader>(options.meta);
    if (options.short_key_ranges_option != nullptr) { // logical split.
        seg_options.short_key_ranges = options.short_key_ranges_option->short_key_ranges;
    }
    seg_options.asc_hint = options.asc_hint;
    if (options.runtime_state != nullptr) {
        seg_options.is_cancelled = &options.runtime_state->cancelled_ref();
    }
    seg_options.prune_column_after_index_filter = options.prune_column_after_index_filter;
    seg_options.enable_gin_filter = options.enable_gin_filter;
    seg_options.has_preaggregation = options.has_preaggregation;

    auto segment_schema = schema;
    // Append the columns with delete condition to segment schema.
    std::set<ColumnId> delete_columns;
    seg_options.delete_predicates.get_column_ids(&delete_columns);
    for (ColumnId cid : delete_columns) {
        const TabletColumn& col = options.tablet_schema->column(cid);
        if (segment_schema.get_field_by_name(std::string(col.name())) == nullptr) {
            auto f = ChunkHelper::convert_field(cid, col);
            segment_schema.append(std::make_shared<Field>(std::move(f)));
        }
    }

    std::vector<ChunkIteratorPtr> tmp_seg_iters;
    tmp_seg_iters.reserve(num_segments());
    if (options.stats) {
        options.stats->segments_read_count += num_segments();
    }
    for (auto& seg_ptr : segments()) {
        if (seg_ptr->num_rows() == 0) {
            continue;
        }

        if (options.rowid_range_option != nullptr) { // physical split.
            auto [rowid_range, is_first_split_of_segment] =
                    options.rowid_range_option->get_segment_rowid_range(this, seg_ptr.get());
            if (rowid_range == nullptr) {
                continue;
            }
            seg_options.rowid_range_option = std::move(rowid_range);
            seg_options.is_first_split_of_segment = is_first_split_of_segment;
        } else if (options.short_key_ranges_option != nullptr) { // logical split.
            seg_options.is_first_split_of_segment = options.short_key_ranges_option->is_first_split_of_tablet;
        } else {
            seg_options.is_first_split_of_segment = true;
        }

        auto res = seg_ptr->new_iterator(segment_schema, seg_options);
        if (res.status().is_end_of_file()) {
            continue;
        }
        if (!res.ok()) {
            return res.status();
        }
        if (segment_schema.num_fields() > schema.num_fields()) {
            tmp_seg_iters.emplace_back(new_projection_iterator(schema, std::move(res).value()));
        } else {
            tmp_seg_iters.emplace_back(std::move(res).value());
        }
    }

    if (!tmp_seg_iters.empty()) {
        if (rowset_meta()->is_segments_overlapping()) {
            for (auto& iter : tmp_seg_iters) {
                auto wrapper = std::make_shared<SegmentIteratorWrapper>(shared_from_this(), std::move(iter));
                segment_iterators->emplace_back(std::move(wrapper));
            }
        } else {
            auto iter = new_union_iterator(std::move(tmp_seg_iters));
            auto wrapper = std::make_shared<SegmentIteratorWrapper>(shared_from_this(), std::move(iter));
            segment_iterators->emplace_back(std::move(wrapper));
        }
    }
    return Status::OK();
}

StatusOr<std::vector<ChunkIteratorPtr>> Rowset::get_segment_iterators2(const Schema& schema,
                                                                       const TabletSchemaCSPtr& tablet_schema,
                                                                       KVStore* meta, int64_t version,
                                                                       OlapReaderStatistics* stats, KVStore* dcg_meta,
                                                                       size_t chunk_size) {
    RETURN_IF_ERROR(load());

    SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(_rowset_path));
    seg_options.stats = stats;
    seg_options.is_primary_keys = meta != nullptr;
    seg_options.tablet_id = rowset_meta()->tablet_id();
    seg_options.rowset_id = rowset_meta()->get_rowset_seg_id();
    seg_options.rowset_path = _rowset_path;
    seg_options.version = version;
    seg_options.tablet_schema = tablet_schema;
    seg_options.delvec_loader = std::make_shared<LocalDelvecLoader>(meta);
    seg_options.dcg_loader = std::make_shared<LocalDeltaColumnGroupLoader>(meta != nullptr ? meta : dcg_meta);
    if (chunk_size > 0) {
        seg_options.chunk_size = chunk_size;
    }
    seg_options.read_by_generated_column_adding = (dcg_meta != nullptr);

    std::vector<ChunkIteratorPtr> seg_iterators(num_segments());
    TabletSegmentId tsid;
    tsid.tablet_id = rowset_meta()->tablet_id();
    for (int64_t i = 0; i < num_segments(); i++) {
        auto& seg_ptr = segments()[i];
        if (seg_ptr->num_rows() == 0) {
            seg_iterators[i] = new_empty_iterator(schema, config::vector_chunk_size);
            continue;
        }
        auto res = seg_ptr->new_iterator(schema, seg_options);
        if (res.status().is_end_of_file()) {
            seg_iterators[i] = new_empty_iterator(schema, config::vector_chunk_size);
            continue;
        }
        if (!res.ok()) {
            return res.status();
        }
        seg_iterators[i] = std::move(res).value();
    }
    return seg_iterators;
}

StatusOr<std::vector<ChunkIteratorPtr>> Rowset::get_update_file_iterators(const Schema& schema,
                                                                          OlapReaderStatistics* stats) {
    SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(_rowset_path));
    seg_options.stats = stats;
    seg_options.tablet_id = rowset_meta()->tablet_id();
    seg_options.rowset_id = rowset_meta()->get_rowset_seg_id();

    std::vector<ChunkIteratorPtr> seg_iterators(num_update_files());
    TabletSegmentId tsid;
    tsid.tablet_id = rowset_meta()->tablet_id();
    for (int64_t i = 0; i < num_update_files(); i++) {
        // open update file
        std::string seg_path = segment_upt_file_path(_rowset_path, rowset_id(), i);
        FileInfo seg_info{.path = seg_path, .encryption_meta = rowset_meta()->get_uptfile_encryption_meta(i)};
        ASSIGN_OR_RETURN(auto seg_ptr, Segment::open(seg_options.fs, seg_info, i, _schema));
        if (seg_ptr->num_rows() == 0) {
            seg_iterators[i] = new_empty_iterator(schema, config::vector_chunk_size);
            continue;
        }
        // create iterator
        auto res = seg_ptr->new_iterator(schema, seg_options);
        if (res.status().is_end_of_file()) {
            seg_iterators[i] = new_empty_iterator(schema, config::vector_chunk_size);
            continue;
        }
        if (!res.ok()) {
            return res.status();
        }
        seg_iterators[i] = std::move(res).value();
    }
    return seg_iterators;
}

StatusOr<ChunkIteratorPtr> Rowset::get_update_file_iterator(const Schema& schema, uint32_t update_file_id,
                                                            OlapReaderStatistics* stats) {
    SegmentReadOptions seg_options;
    ASSIGN_OR_RETURN(seg_options.fs, FileSystem::CreateSharedFromString(_rowset_path));
    seg_options.stats = stats;
    seg_options.tablet_id = rowset_meta()->tablet_id();
    seg_options.rowset_id = rowset_meta()->get_rowset_seg_id();
    seg_options.rowset_path = _rowset_path;

    // open update file
    DCHECK(update_file_id < num_update_files());
    std::string seg_path = segment_upt_file_path(_rowset_path, rowset_id(), update_file_id);
    FileInfo seg_info{.path = seg_path, .encryption_meta = rowset_meta()->get_uptfile_encryption_meta(update_file_id)};
    ASSIGN_OR_RETURN(auto seg_ptr, Segment::open(seg_options.fs, seg_info, update_file_id, _schema));
    if (seg_ptr->num_rows() == 0) {
        return new_empty_iterator(schema, config::vector_chunk_size);
    }
    // create iterator
    auto res = seg_ptr->new_iterator(schema, seg_options);
    if (res.status().is_end_of_file()) {
        return new_empty_iterator(schema, config::vector_chunk_size);
    }
    if (!res.ok()) {
        return res.status();
    }
    return std::move(res).value();
}

Status Rowset::get_segment_sk_index(std::vector<std::string>* sk_index_values) {
    RETURN_IF_ERROR(load());
    for (auto& segment : _segments) {
        RETURN_IF_ERROR(segment->get_short_key_index(sk_index_values));
    }
    return Status::OK();
}

static int compare_row(const Chunk& l, size_t l_row_id, const Chunk& r, size_t r_row_id) {
    const size_t ncolumn = l.num_columns();
    for (size_t i = 0; i < ncolumn; i++) {
        auto v = l.columns()[i]->compare_at(l_row_id, r_row_id, *r.columns()[i], -1);
        if (v != 0) {
            return v;
        }
    }
    return 0;
}

static Status report_duplicate(const Chunk& chunk, size_t idx, int64_t row_id0, int64_t row_id1) {
    return Status::Corruption(
            strings::Substitute("duplicate row $0 row:$1==row:$2", chunk.debug_row(idx), row_id0, row_id1));
}

static Status report_unordered(const Chunk& chunk0, size_t idx0, int64_t row_id0, const Chunk& chunk1, size_t idx1,
                               int64_t row_id1) {
    return Status::Corruption(strings::Substitute("unordered row row:$0 $1 > row:$2 $3", row_id0,
                                                  chunk0.debug_row(idx0), row_id1, chunk1.debug_row(idx1)));
}

static Status is_ordered(ChunkIteratorPtr& iter, bool unique) {
    ChunkUniquePtr chunks[2];
    chunks[0] = ChunkHelper::new_chunk(iter->schema(), iter->chunk_size());
    chunks[1] = ChunkHelper::new_chunk(iter->schema(), iter->chunk_size());
    size_t chunk_idx = 0;
    int64_t row_idx = 0;
    while (true) {
        auto& cur = *chunks[chunk_idx];
        cur.reset();
        auto st = iter->get_next(&cur);
        if (st.is_end_of_file()) {
            break;
        } else if (!st.ok()) {
            return st;
        }
        auto& prev = *chunks[(chunk_idx + 1) % 2];
        // check first row in this chunk is GT/GE last row in previous chunk
        if (prev.has_rows()) {
            auto cmp = compare_row(prev, prev.num_rows() - 1, cur, 0);
            if (cmp == 0) {
                if (unique) {
                    return report_duplicate(cur, 0, row_idx - 1, row_idx);
                }
            } else if (cmp > 0) {
                return report_unordered(prev, prev.num_rows() - 1, row_idx - 1, cur, 0, row_idx);
            }
        }
        // check rows in this chunk is ordered
        for (size_t i = 1; i < cur.num_rows(); i++) {
            auto cmp = compare_row(cur, i - 1, cur, i);
            if (cmp == 0) {
                if (unique) {
                    return report_duplicate(cur, i, row_idx + i - 1, row_idx + i);
                }
            } else if (cmp > 0) {
                return report_unordered(cur, i - 1, row_idx + i - 1, cur, i, row_idx + i);
            }
        }
        row_idx += cur.num_rows();
        chunk_idx = (chunk_idx + 1) % 2;
    }
    return Status::OK();
}

Status Rowset::verify() {
    vector<ColumnId> key_columns;
    vector<ColumnId> order_columns;
    bool is_pk_ordered = false;
    for (int i = 0; i < _schema->num_key_columns(); i++) {
        key_columns.push_back(i);
    }
    if (!_schema->sort_key_idxes().empty() && key_columns != _schema->sort_key_idxes()) {
        order_columns = _schema->sort_key_idxes();
    } else {
        order_columns = key_columns;
        is_pk_ordered = _schema->keys_type() == PRIMARY_KEYS;
    }
    Schema order_schema = ChunkHelper::convert_schema(_schema, order_columns);
    RowsetReadOptions rs_opts;
    OlapReaderStatistics stats;
    rs_opts.sorted = false;
    rs_opts.stats = &stats;
    rs_opts.use_page_cache = false;
    rs_opts.tablet_schema = _schema;

    std::vector<ChunkIteratorPtr> iters;
    RETURN_IF_ERROR(get_segment_iterators(order_schema, rs_opts, &iters));

    // overlapping segments will return multiple iterators, so segment idx is known
    Status st;
    if (rowset_meta()->is_segments_overlapping()) {
        for (size_t i = 0; i < iters.size(); i++) {
            st = is_ordered(iters[i], is_pk_ordered);
            if (!st.ok()) {
                st = st.clone_and_append(strings::Substitute("segment:$0", i));
                break;
            }
        }
    } else {
        if (iters.empty()) {
            st = Status::OK();
        } else if (iters.size() != 1) {
            st = Status::Corruption("non-overlapping segments should return one iterator");
        } else {
            st = is_ordered(iters[0], is_pk_ordered);
        }
    }
    if (!st.ok()) {
        (void)st.clone_and_append(strings::Substitute("rowset:$0 path:$1", rowset_id().to_string(), rowset_path()));
    }
    return st;
}

} // namespace starrocks
