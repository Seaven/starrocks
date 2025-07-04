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

package com.starrocks.metric;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.AutoCloseableLock;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.metric.Metric.MetricUnit;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.TaskRunScheduler;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;

public final class MaterializedViewMetricsEntity implements IMaterializedViewMetricsEntity {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewMetricsEntity.class);

    private final MvId mvId;
    private final MetricRegistry metricRegistry;
    private final List<Metric> metrics = Lists.newArrayList();

    // refresh
    // increased once the materialized view's refresh job is triggered.
    public LongCounterMetric counterRefreshJobTotal;
    // increased only if the materialized view's refresh job is success refreshed.
    public LongCounterMetric counterRefreshJobSuccessTotal;
    // increased once the materialized view's refresh job is failed.
    public LongCounterMetric counterRefreshJobFailedTotal;
    // increased once the materialized view's refresh job is not be triggered because of the refreshed data is empty.
    public LongCounterMetric counterRefreshJobEmptyTotal;
    // increased once the materialized view's refresh job checks whether the base table is changed or not.
    public LongCounterMetric counterRefreshJobRetryCheckChangedTotal;

    // query
    // increased if the materialized view is considered in the preprocess for one query.
    public LongCounterMetric counterQueryConsideredTotal;
    // increased if the materialized view is successes to be rewritten from query, but it may be missed in the
    // final plan because of the cost.
    public LongCounterMetric counterQueryMatchedTotal;
    // only increased if the materialized view is rewritten and not be queried directly
    public LongCounterMetric counterQueryHitTotal;
    // increased once the materialized view is used in the final plan no matter it is queried directly or rewritten.
    public LongCounterMetric counterQueryMaterializedViewTotal;
    // increased if the materialized view is successes to be rewritten from query by text based rewrite, and it will increase
    // the counter query hit total.
    public LongCounterMetric counterQueryTextBasedMatchedTotal;

    // gauge
    // the current pending refresh jobs for the materialized view
    public GaugeMetric<Long> counterRefreshPendingJobs;
    // the current running refresh jobs for the materialized view
    public GaugeMetric<Long> counterRefreshRunningJobs;
    // the current materialized view's row count
    public GaugeMetric<Long> counterRowNums;
    // the current materialized view's storage size, unit: byte
    public GaugeMetric<Long> counterStorageSize;
    // the current materialized view is active or not, 0: active, 1: inactive
    public GaugeMetric<Integer> counterInactiveState;

    // the current materialized view's partition count, 0 if the materialized view is not partitioned
    public GaugeMetric<Integer> counterPartitionCount;

    // histogram(ms)
    // record the materialized view's refresh job duration only if it's refreshed successfully.
    public Histogram histRefreshJobDuration;

    public Optional<String> dbNameOpt = Optional.empty();
    public Optional<String> mvNameOpt = Optional.empty();

    public MaterializedViewMetricsEntity(MetricRegistry metricRegistry, MvId mvId) {
        this.metricRegistry = metricRegistry;
        this.mvId = mvId;
        initMaterializedViewMetrics();
        initDbAndTableName();
    }

    public boolean initDbAndTableName() {
        if (dbNameOpt.isPresent() && mvNameOpt.isPresent()) {
            return true;
        }

        GlobalStateMgr globalStateMgr = GlobalStateMgr.getCurrentState();
        Database db = globalStateMgr.getLocalMetastore().getDb(mvId.getDbId());
        if (db != null) {
            dbNameOpt = Optional.of(db.getFullName());
            Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), mvId.getId());
            if (table != null) {
                mvNameOpt = Optional.of(table.getName());
                return true;
            }
        }
        return false;
    }

    @Override
    public List<Metric> getMetrics() {
        return metrics;
    }

    protected void initMaterializedViewMetrics() {
        // refresh metrics
        counterRefreshJobTotal = new LongCounterMetric("mv_refresh_jobs", MetricUnit.REQUESTS,
                    "total materialized view's refresh jobs");
        metrics.add(counterRefreshJobTotal);
        counterRefreshJobSuccessTotal = new LongCounterMetric("mv_refresh_total_success_jobs", MetricUnit.REQUESTS,
                    "total materialized view's refresh success jobs");
        metrics.add(counterRefreshJobSuccessTotal);
        counterRefreshJobFailedTotal = new LongCounterMetric("mv_refresh_total_failed_jobs", MetricUnit.REQUESTS,
                    "total materialized view's refresh failed jobs");
        metrics.add(counterRefreshJobFailedTotal);
        counterRefreshJobEmptyTotal = new LongCounterMetric("mv_refresh_total_empty_jobs", MetricUnit.REQUESTS,
                    "total materialized view's refresh empty jobs");
        metrics.add(counterRefreshJobEmptyTotal);
        counterRefreshJobRetryCheckChangedTotal = new LongCounterMetric("mv_refresh_total_retry_meta_count", MetricUnit.REQUESTS,
                    "total materialized view's retry to check table change count");
        metrics.add(counterRefreshJobRetryCheckChangedTotal);

        // query metrics
        counterQueryMaterializedViewTotal = new LongCounterMetric("mv_query_total_count", MetricUnit.REQUESTS,
                    "total materialized view's query count");
        metrics.add(counterQueryMaterializedViewTotal);
        counterQueryHitTotal = new LongCounterMetric("mv_query_total_hit_count", MetricUnit.REQUESTS,
                    "total hit materialized view's query count");
        metrics.add(counterQueryHitTotal);
        counterQueryConsideredTotal = new LongCounterMetric("mv_query_total_considered_count", MetricUnit.REQUESTS,
                    "total considered materialized view's query count");
        metrics.add(counterQueryConsideredTotal);
        counterQueryMatchedTotal = new LongCounterMetric("mv_query_total_matched_count", MetricUnit.REQUESTS,
                    "total matched materialized view's query count");
        metrics.add(counterQueryMatchedTotal);
        // text based rewrite
        counterQueryTextBasedMatchedTotal = new LongCounterMetric("mv_query_total_text_based_matched_count",
                    MetricUnit.REQUESTS, "total text based matched materialized view's query count");
        metrics.add(counterQueryTextBasedMatchedTotal);

        // histogram metrics
        try {
            Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
            MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                        .getTable(db.getId(), mvId.getId());
            HistogramMetric histogram = new HistogramMetric("mv_refresh_duration");
            histogram.addLabel(new MetricLabel("db_name", db.getFullName()));
            histogram.addLabel(new MetricLabel("mv_name", mv.getName()));
            histRefreshJobDuration = metricRegistry.histogram(histogram.getHistogramName(), () -> histogram);
        } catch (Exception e) {
            LOG.warn("Ignore histogram metrics for materialized view: {}", mvId);
        }

        // gauge metrics
        counterRefreshPendingJobs = new GaugeMetric<Long>("mv_refresh_pending_jobs", MetricUnit.NOUNIT,
                    "current materialized view pending refresh jobs number") {
            @Override
            public Long getValue() {
                String mvTaskName = TaskBuilder.getMvTaskName(mvId.getId());
                TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
                if (taskManager == null) {
                    return 0L;
                }
                if (!taskManager.containTask(mvTaskName)) {
                    return 0L;
                }
                Long taskId = taskManager.getTask(mvTaskName).getId();
                TaskRunScheduler taskRunScheduler = taskManager.getTaskRunScheduler();
                return taskRunScheduler.getTaskIdPendingTaskRunCount(taskId);
            }
        };
        metrics.add(counterRefreshPendingJobs);

        counterRefreshRunningJobs = new GaugeMetric<Long>("mv_refresh_running_jobs", MetricUnit.NOUNIT,
                    "current materialized view running refresh jobs number") {
            @Override
            public Long getValue() {
                String mvTaskName = TaskBuilder.getMvTaskName(mvId.getId());
                TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
                if (taskManager == null) {
                    return 0L;
                }
                if (!taskManager.containTask(mvTaskName)) {
                    return 0L;
                }
                Long taskId = taskManager.getTask(mvTaskName).getId();
                TaskRunScheduler taskRunScheduler = taskManager.getTaskRunManager().getTaskRunScheduler();
                if (taskRunScheduler.isTaskRunning(taskId)) {
                    return 1L;
                } else {
                    return 0L;
                }
            }
        };
        metrics.add(counterRefreshRunningJobs);

        counterRowNums = new GaugeMetric<Long>("mv_row_count", MetricUnit.NOUNIT,
                    "current materialized view's row count") {
            @Override
            public Long getValue() {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
                if (db == null) {
                    return 0L;
                }
                Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), mvId.getId());
                if (table == null || !table.isMaterializedView()) {
                    return 0L;
                }

                MaterializedView mv = (MaterializedView) table;
                try (AutoCloseableLock ignore = new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(table.getId()),
                            LockType.READ)) {
                    return mv.getRowCount();
                } catch (Exception e) {
                    return 0L;
                }
            }
        };
        metrics.add(counterRowNums);

        counterStorageSize = new GaugeMetric<Long>("mv_storage_size", MetricUnit.NOUNIT,
                    "current materialized view's storage size") {
            @Override
            public Long getValue() {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
                if (db == null) {
                    return 0L;
                }
                Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), mvId.getId());
                if (table == null || !table.isMaterializedView()) {
                    return 0L;
                }

                MaterializedView mv = (MaterializedView) table;
                try (AutoCloseableLock ignore =
                            new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(table.getId()), LockType.READ)) {
                    return mv.getDataSize();
                } catch (Exception e) {
                    return 0L;
                }
            }
        };
        metrics.add(counterStorageSize);

        counterInactiveState = new GaugeMetric<Integer>("mv_inactive_state", MetricUnit.NOUNIT,
                    "current materialized view's inactive or not, 0: active, 1: inactive") {
            @Override
            public Integer getValue() {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
                if (db == null) {
                    return 0;
                }
                Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), mvId.getId());
                if (table == null || !table.isMaterializedView()) {
                    return 0;
                }
                try {
                    MaterializedView mv = (MaterializedView) table;
                    return mv.isActive() ? 0 : 1;
                } catch (Exception e) {
                    return 0;
                }
            }
        };
        metrics.add(counterInactiveState);

        counterPartitionCount = new GaugeMetric<Integer>("mv_partition_count", MetricUnit.NOUNIT,
                    "current materialized view's partition count, 0 if the materialized view is not partitioned") {
            @Override
            public Integer getValue() {
                Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(mvId.getDbId());
                if (db == null) {
                    return 0;
                }
                Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), mvId.getId());
                if (table == null || !table.isMaterializedView()) {
                    return 0;
                }
                MaterializedView mv = (MaterializedView) table;
                if (!mv.isPartitionedTable()) {
                    return 0;
                }

                try (AutoCloseableLock ignore =
                            new AutoCloseableLock(new Locker(), db.getId(), Lists.newArrayList(table.getId()), LockType.READ)) {
                    return mv.getPartitions().size();
                } catch (Exception e) {
                    return 0;
                }
            }
        };
        metrics.add(counterPartitionCount);
    }

    @Override
    public void increaseQueryConsideredCount(long count) {
        this.counterQueryConsideredTotal.increase(count);
    }

    @Override
    public void increaseQueryMatchedCount(long count) {
        this.counterQueryMatchedTotal.increase(count);
    }

    @Override
    public void increaseQueryTextBasedMatchedCount(long count) {
        this.counterQueryTextBasedMatchedTotal.increase(count);
    }

    @Override
    public void increaseQueryHitCount(long count) {
        this.counterQueryHitTotal.increase(count);
    }

    @Override
    public void increaseQueryMaterializedViewCount(long count) {
        this.counterQueryMaterializedViewTotal.increase(count);
    }

    @Override
    public void increaseRefreshJobStatus(Constants.TaskRunState status) {
        this.counterRefreshJobTotal.increase(1L);
        switch (status) {
            case SKIPPED:
                this.counterRefreshJobEmptyTotal.increase(1L);
                break;
            case SUCCESS:
                this.counterRefreshJobSuccessTotal.increase(1L);
                break;
            case FAILED:
                this.counterRefreshJobFailedTotal.increase(1L);
                break;
        }
    }

    @Override
    public void increaseRefreshRetryMetaCount(Long retryNum) {
        this.counterRefreshJobRetryCheckChangedTotal.increase(retryNum);
    }

    @Override
    public void updateRefreshDuration(long duration) {
        this.histRefreshJobDuration.update(duration);
    }
}

