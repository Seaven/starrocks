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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.QueryOptimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class MvRewriteStrategyTest extends MVTestBase {
    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        starRocksAssert.withTable(cluster, "test_base_part");
        starRocksAssert.withTable(cluster, "table_with_partition");
    }

    private OptExpression optimize(Optimizer optimizer, String sql) {
        try {
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
            QueryStatement queryStatement = (QueryStatement) stmt;
            LogicalPlan logicalPlan =
                    new RelationTransformer(optimizer.getContext().getColumnRefFactory(), connectContext)
                    .transformWithSelectLimit(queryStatement.getQueryRelation());
            return optimizer.optimize(logicalPlan.getRoot(), new PhysicalPropertySet(),
                    new ColumnRefSet(logicalPlan.getOutputColumn()));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
            return null;
        }
    }

    @Test
    public void testSingleTableRewriteStrategy() throws Exception {
        createAndRefreshMv("create materialized view mv1 " +
                " partition by id_date" +
                " distributed by random " +
                " as" +
                " select t1a, id_date, t1b from table_with_partition");
        String sql =  "select t1a, id_date, t1b from table_with_partition";
        QueryOptimizer optimizer = (QueryOptimizer) OptimizerFactory.create(
                OptimizerFactory.mockContext(connectContext, new ColumnRefFactory()));
        OptExpression optExpression = optimize(optimizer, sql);
        Assertions.assertTrue(optExpression != null);
        MvRewriteStrategy mvRewriteStrategy = optimizer.getMvRewriteStrategy();
        Assertions.assertTrue(mvRewriteStrategy.enableMultiTableRewrite == false);
        Assertions.assertTrue(mvRewriteStrategy.enableSingleTableRewrite == true);
        Assertions.assertTrue(mvRewriteStrategy.enableMaterializedViewRewrite == true);
        Assertions.assertTrue(mvRewriteStrategy.enableForceRBORewrite == true);
        Assertions.assertTrue(mvRewriteStrategy.enableViewBasedRewrite == false);
    }
}