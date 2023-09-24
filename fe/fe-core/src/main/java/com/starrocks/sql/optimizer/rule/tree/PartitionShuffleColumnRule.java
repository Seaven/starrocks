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


package com.starrocks.sql.optimizer.rule.tree;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.DistributionCol;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.HashDistributionSpec;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalDistributionOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalJoinOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class PartitionShuffleColumnRule implements TreeRewriteRule {

    private static final Logger LOG = LogManager.getLogger(PartitionShuffleColumnRule.class);

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SessionVariable sessionVariable = taskContext.getOptimizerContext().getSessionVariable();
        if (!sessionVariable.isEnablePartitionColumnShuffleOptimization()) {
            return root;
        }

        PartitionShuffleColumnVisitor visitor = new PartitionShuffleColumnVisitor(taskContext);
        visitor.rewrite(root);
        return root;
    }

    private static class PartitionShuffleColumnVisitor extends OptExpressionVisitor<OptExpression, DistributionContext> {
        private final ColumnRefFactory factory;
        private final SessionVariable sessionVariable;

        public PartitionShuffleColumnVisitor(TaskContext taskContext) {
            this.factory = taskContext.getOptimizerContext().getColumnRefFactory();
            this.sessionVariable = taskContext.getOptimizerContext().getSessionVariable();
        }

        public void rewrite(OptExpression root) {
            DistributionContext rootContext = new DistributionContext(false);
            root.getOp().accept(this, root, rootContext);
        }

        @Override
        public OptExpression visit(OptExpression optExpression, DistributionContext context) {
            for (int i = 0; i < optExpression.arity(); ++i) {
                optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), context);
            }
            return optExpression;
        }

        @Override
        public OptExpression visitPhysicalDistribution(OptExpression optExpression, DistributionContext context) {
            if (context.isCollect()) {
                context.addDistribution(optExpression);
            }

            for (int i = 0; i < optExpression.arity(); ++i) {
                optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i), context);
            }
            return optExpression;
        }

        /**
         * Reorder distribution columns to make sure the partition column are in the fist.
         */
        private void optDistributionByTablePartitionColumn(ColumnRefOperator partitionCol,
                                                           DistributionContext childContext) {
            // only join can be return more distribution
            if (childContext.distributionList.size() < 2) {
                return;
            }

            Preconditions.checkState(childContext.distributionList.stream()
                    .allMatch(s -> s.getDistributionSpec().getType() == DistributionSpec.DistributionType.SHUFFLE));

            List<HashDistributionDesc> descs = childContext.distributionList.stream()
                    .map(s -> ((HashDistributionSpec) s.getDistributionSpec()).getHashDistributionDesc()).collect(
                            Collectors.toList());
            Preconditions.checkState(descs.stream().mapToInt(d -> d.getDistributionCols().size()).distinct().count() == 1);

            int index = findPartitionColumnInDistributions(descs, partitionCol);
            if (index < 0) {
                return;
            }

            for (int j = 0; j < descs.size(); j++) {
                descs.get(j).setTablePartitionColumnIds(Lists.newArrayList(index));
            }
        }

        private int findPartitionColumnInDistributions(List<HashDistributionDesc> descs,
                                                       ColumnRefOperator partitionCol) {
            for (HashDistributionDesc desc : descs) {
                List<DistributionCol> distributionCols = desc.getDistributionCols();
                for (int j = 0; j < distributionCols.size(); j++) {
                    DistributionCol col = distributionCols.get(j);
                    if (col.getColId() == partitionCol.getId()) {
                        return j;
                    }
                }
            }
            return -1;
        }

        /**
         * Find partition column of the table.
         */
        private Column findTablePartitionColumn(Table table) {
            // find partition column
            Column partitionColumn = null;
            if (table instanceof OlapTable) {
                PartitionInfo info = ((OlapTable) table).getPartitionInfo();
                if (info instanceof RangePartitionInfo) {
                    RangePartitionInfo ri = (RangePartitionInfo) info;
                    if (ri.getPartitionColumns().size() == 1) {
                        partitionColumn = ri.getPartitionColumns().get(0);
                    }
                }
            } else if (table.isHiveTable() || table.isIcebergTable() || table.isHudiTable()) {
                List<Column> partitionColumns = PartitionUtil.getPartitionColumns(table);
                if (partitionColumns.size() == 1) {
                    partitionColumn = ((IcebergTable) table).getPartitionColumns().get(0);
                }
            }
            if (partitionColumn != null) {
                return partitionColumn;
            } else {
                return findTablePartitionColumnByHint(table);
            }
        }

        private Column findTablePartitionColumnByHint(Table table) {
            if (sessionVariable.getShufflePartitionColumnHints() == null ||
                    sessionVariable.getShufflePartitionColumnHints().isEmpty()) {
                return null;
            }
            String[] columnHints = sessionVariable.getShufflePartitionColumnHints().split(";");
            Set<String> columnHintSet = Sets.newHashSet(columnHints);
            for (Column column : table.getColumns()) {
                if (columnHintSet.contains(column.getName())) {
                    return column;
                }
            }
            return null;
        }

        ColumnRefOperator getTablePartitionColumnRefInJoinOnPredicates(OptExpression input,
                                                                       PhysicalJoinOperator join) {
            ColumnRefOperator partitionColumnRef =
                    getTablePartitionColumnRefInJoinOnPredicatesByTable(input, join);
            if (partitionColumnRef != null) {
                return partitionColumnRef;
            }
            return getTablePartitionColumnRefInJoinOnPredicatesByHint(join);
        }

        ColumnRefOperator getTablePartitionColumnRefInJoinOnPredicatesByTable(OptExpression input,
                                                                              PhysicalJoinOperator join) {
            List<PhysicalScanOperator> allScanList = Lists.newArrayList();
            Utils.extractScanOperator(input, allScanList);
            if (allScanList.size() > 1) {
                return null;
            }

            PhysicalScanOperator scan = allScanList.get(0);
            Table table = scan.getTable();
            Column partitionColumn = findTablePartitionColumn(table);
            if (partitionColumn == null) {
                return null;
            }

            for (ColumnRefOperator c : join.getOnPredicate().getColumnRefs()) {
                if (scan.getColRefToColumnMetaMap().containsKey(c)) {
                    if (scan.getColRefToColumnMetaMap().get(c).equals(partitionColumn)) {
                        return c;
                    }
                }
            }
            return null;
        }

        ColumnRefOperator getTablePartitionColumnRefInJoinOnPredicatesByHint(PhysicalJoinOperator join) {
            if (sessionVariable.getShufflePartitionColumnHints() == null ||
                    sessionVariable.getShufflePartitionColumnHints().isEmpty()) {
                return null;
            }
            String[] columnHints = sessionVariable.getShufflePartitionColumnHints().split(";");
            Set<String> columnHintSet = Sets.newHashSet(columnHints);
            for (ColumnRefOperator c : join.getOnPredicate().getColumnRefs()) {
                if (columnHintSet.contains(c.getName())) {
                    return c;
                }
            }
            return null;
        }

        @Override
        public OptExpression visitPhysicalHashJoin(OptExpression optExpression, DistributionContext context) {
            handlePhysicalJoinNode(optExpression);
            for (int i = 0; i < optExpression.arity(); ++i) {
                optExpression.inputAt(i).getOp().accept(this, optExpression.inputAt(i),
                        new DistributionContext(false));
            }
            return optExpression;
        }

        private void handlePhysicalJoinNode(OptExpression optExpression) {
            PhysicalJoinOperator join = (PhysicalJoinOperator) optExpression.getOp();
            if (join.getOnPredicate() == null) {
                return;
            }

            OptExpression left = optExpression.inputAt(0);
            OptExpression right = optExpression.inputAt(1);
            ColumnRefOperator leftPartitionColumn = getTablePartitionColumnRefInJoinOnPredicates(left, join);
            ColumnRefOperator rightPartitionColumn = getTablePartitionColumnRefInJoinOnPredicates(right, join);
            if (leftPartitionColumn == null && rightPartitionColumn == null) {
                return;
            }

            DistributionContext lc = new DistributionContext(true);
            DistributionContext rc = new DistributionContext(true);
            optExpression.getInputs().get(0).getOp().accept(this, optExpression.getInputs().get(0), lc);
            optExpression.getInputs().get(1).getOp().accept(this, optExpression.getInputs().get(1), rc);
            if (lc.isShuffle() && rc.isShuffle()) {
                DistributionContext dc = new DistributionContext(false);
                dc.add(lc);
                dc.add(rc);
                if (leftPartitionColumn != null) {
                    optDistributionByTablePartitionColumn(leftPartitionColumn, dc);
                } else if (rightPartitionColumn != null) {
                    optDistributionByTablePartitionColumn(rightPartitionColumn, dc);
                }
            }
        }
    }

    public static class DistributionContext {
        public final List<PhysicalDistributionOperator> distributionList = Lists.newArrayList();
        private final boolean isCollect;

        public DistributionContext(boolean isCollect) {
            this.isCollect = isCollect;
        }

        public boolean isCollect() {
            return isCollect;
        }

        public void addDistribution(OptExpression optExpression) {
            Preconditions.checkState(optExpression.getOp().getOpType() == OperatorType.PHYSICAL_DISTRIBUTION);
            this.distributionList.add((PhysicalDistributionOperator) optExpression.getOp());
        }

        public void add(DistributionContext other) {
            this.distributionList.addAll(other.distributionList);
        }

        private boolean isShuffle() {
            if (distributionList.isEmpty()) {
                return false;
            }

            for (PhysicalDistributionOperator d : distributionList) {
                if (d.getDistributionSpec().getType() != DistributionSpec.DistributionType.SHUFFLE) {
                    return false;
                }

                HashDistributionDesc desc = ((HashDistributionSpec) d.getDistributionSpec()).getHashDistributionDesc();
                if (!desc.isShuffle() && !desc.isShuffleEnforce()) {
                    return false;
                }
            }
            return true;
        }
    }
}
