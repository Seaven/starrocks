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

package com.starrocks.qe.recursivecte;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.TableName;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.SimpleExecutor;
import com.starrocks.sql.analyzer.Analyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.PlannerMetaLocker;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.ColumnDef;
import com.starrocks.sql.ast.CreateTemporaryTableAsSelectStmt;
import com.starrocks.sql.ast.CreateTemporaryTableStmt;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.KeysDesc;
import com.starrocks.sql.ast.OrderByElement;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.RandomDistributionDesc;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.IntLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TypeDef;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.type.IntegerType;

import java.util.List;
import java.util.Map;

public class RecursiveCTEExecutor {
    private record RecursiveCTEGroup(QueryStatement startStmt, QueryStatement recursiveStmt,
                                     CreateTemporaryTableStmt tempTableStmt) {
    }

    private Map<String, RecursiveCTEGroup> recursiveCTEGroups = Maps.newLinkedHashMap();

    private Map<String, TableName> cteTempTableMap = Maps.newHashMap();

    private ConnectContext connectContext;

    public StatementBase splitOuterStmt(StatementBase stmt, ConnectContext session) {
        analyze(stmt, session);
        RecursiveCTESplitter splitter = new RecursiveCTESplitter();
        splitter.visit(stmt, null);
        return stmt;
    }

    private void analyze(StatementBase stmt, ConnectContext context) {
        try (PlannerMetaLocker locker = new PlannerMetaLocker(context, stmt)) {
            locker.lock();
            Analyzer.analyze(stmt, context);
        }
    }

    public void prepareRecursiveCTE() throws Exception {
        SimpleExecutor executor = new SimpleExecutor("RecursiveCTE-Executor", TResultSinkType.MYSQL_PROTOCAL);
        for (String cteName : recursiveCTEGroups.keySet()) {
            RecursiveCTEGroup group = recursiveCTEGroups.get(cteName);
            TableName tempTableName = cteTempTableMap.get(cteName);
            CreateTemporaryTableAsSelectStmt cttas = new CreateTemporaryTableAsSelectStmt(group.tempTableStmt,
                    group.startStmt.getQueryRelation().getColumnOutputNames(), group.startStmt, new NodePosition(1, 1));
            analyze(cttas, connectContext);
            DDLStmtExecutor.execute(cttas, connectContext);
            for (int i = 0; i < connectContext.getSessionVariable().getRecursiveCteMaxDepth(); i++) {
                InsertStmt insert = updateRecursiveCTE(tempTableName, group.recursiveStmt, i);
                analyze(insert, connectContext);
                executor.executeDML(insert);
            }
        }
    }

    private InsertStmt updateRecursiveCTE(TableName tempTableName, QueryStatement select, int loops) {
        int index = select.getQueryRelation().getOutputExpression().size() - 1;
        select.getQueryRelation().getOutputExpression().set(index, new IntLiteral(loops + 1));
        new RecursiveCTEPredicateUpdater(loops, tempTableName).visit(select.getQueryRelation());
        return new InsertStmt(tempTableName, select);
    }

    private static class RecursiveCTEPredicateUpdater extends AstTraverser<Void, Void> {
        private final int currentLevel;
        private final TableName tempTableName;

        public RecursiveCTEPredicateUpdater(int currentLevel, TableName tempTableName) {
            this.currentLevel = currentLevel;
            this.tempTableName = tempTableName;
        }

        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            super.visitSelect(node, context);
            if (node.getRelation() instanceof TableRelation tableRelation) {
                if (tempTableName.equals(tableRelation.getName())) {
                    // add predicate to filter current level
                    IntLiteral levelLiteral = new IntLiteral(currentLevel);
                    Expr equalsLevel = new BinaryPredicate(BinaryType.EQ,
                            new SlotRef(tableRelation.getName(), "_cte_level"), levelLiteral);
                    if (node.getPredicate() == null) {
                        node.setPredicate(equalsLevel);
                    } else {
                        Expr newPredicate =
                                new CompoundPredicate(CompoundPredicate.Operator.AND, node.getPredicate(), equalsLevel);
                        node.setPredicate(newPredicate);
                    }
                }
            }
            return null;
        }
    }

    private class RecursiveCTESplitter extends AstTraverser<Void, Void> {
        @Override
        public Void visitSelect(SelectRelation node, Void context) {
            if (node.hasWithClause()) {
                List<CTERelation> cteRelations = Lists.newArrayList();
                for (CTERelation cteRelation : node.getCteRelations()) {
                    if (cteRelation.isRecursive()) {
                        visit(cteRelation, context);
                    } else {
                        cteRelations.add(cteRelation);
                    }
                }
                node.getCteRelations().clear();
                node.getCteRelations().addAll(cteRelations);
            }

            if (node.getOrderBy() != null) {
                for (OrderByElement orderByElement : node.getOrderBy()) {
                    visit(orderByElement.getExpr(), context);
                }
            }

            if (node.getOutputExpression() != null) {
                node.getOutputExpression().forEach(x -> visit(x, context));
            }

            if (node.getPredicate() != null) {
                visit(node.getPredicate(), context);
            }

            if (node.getGroupBy() != null) {
                node.getGroupBy().forEach(x -> visit(x, context));
            }

            if (node.getAggregate() != null) {
                node.getAggregate().forEach(x -> visit(x, context));
            }

            if (node.getHaving() != null) {
                visit(node.getHaving(), context);
            }

            if (node.getRelation() instanceof CTERelation cteRelation &&
                    cteRelation.isRecursive() &&
                    recursiveCTEGroups.containsKey(cteRelation.getName())) {
                TableName name = recursiveCTEGroups.get(cteRelation.getName()).tempTableStmt.getDbTbl();
                node.setRelation(new TableRelation(name, null, null, null));
            } else {
                visit(node.getRelation(), context);
            }
            return null;
        }

        @Override
        public Void visitCTE(CTERelation node, Void context) {
            Preconditions.checkState(node.isRecursive());
            if (!node.isRecursive() || !node.isAnchor()) {
                return null;
            }

            Preconditions.checkState(node.getCteQueryStatement().getQueryRelation() instanceof SetOperationRelation);
            if (!(node.getCteQueryStatement().getQueryRelation() instanceof UnionRelation unionRelation)) {
                return null;
            }
            if (unionRelation.getRelations().size() < 2) {
                return null;
            }

            // create temporary table for recursive cte
            List<ColumnDef> columnDefs = Lists.newArrayList();
            for (Field allField : node.getRelationFields().getAllFields()) {
                columnDefs.add(new ColumnDef(allField.getName(), new TypeDef(allField.getType()), true));
            }
            columnDefs.add(new ColumnDef("_cte_level", new TypeDef(IntegerType.INT), true));

            TableName tempTableName =
                    new TableName(connectContext.getDatabase(), node.getName() + "_" + System.currentTimeMillis());
            CreateTemporaryTableStmt tempTableStmt = new CreateTemporaryTableStmt(false, false,
                    tempTableName, columnDefs, List.of(), "OLAP", "utf8",
                    new KeysDesc(KeysType.DUP_KEYS, Lists.newArrayList(columnDefs.get(0).getName())),
                    null, new RandomDistributionDesc(), Map.of(), Map.of(),
                    "Temporary Table for Recursive CTE", Lists.newArrayList(), Lists.newArrayList());

            // split recursive cte
            unionRelation.getRelations().get(0).getOutputExpression().add(new IntLiteral(0));
            unionRelation.getRelations().get(0).getColumnOutputNames().add("_cte_level");
            QueryStatement start = new QueryStatement(unionRelation.getRelations().get(0));

            QueryStatement recursive = null;
            if (unionRelation.getRelations().size() == 2) {
                unionRelation.getRelations().get(1).getOutputExpression().add(new IntLiteral(0));
                unionRelation.getRelations().get(1).getColumnOutputNames().add("_cte_level");
                recursive = new QueryStatement(unionRelation.getRelations().get(1));
            } else {
                // @todo: support more than 2 union members
                Preconditions.checkState(false, "Only support 2 union members in recursive CTE");
            }
            visit(recursive.getQueryRelation());

            recursiveCTEGroups.put(node.getName(), new RecursiveCTEGroup(start, recursive, tempTableStmt));
            cteTempTableMap.put(node.getName(), tempTableName);
            return null;
        }

    }
}
