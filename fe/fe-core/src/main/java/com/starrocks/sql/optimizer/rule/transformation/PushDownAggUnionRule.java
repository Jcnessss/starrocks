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


package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.sql.optimizer.operator.AggType.LOCAL;

public class PushDownAggUnionRule extends TransformationRule {


    private static final PushDownAggUnionRule INSTANCE = new PushDownAggUnionRule();

    public static PushDownAggUnionRule getInstance() {
        return INSTANCE;
    }

    public PushDownAggUnionRule() {
        super(RuleType.TF_PUSH_DOWN_AGG_UNION, Pattern.create(OperatorType.LOGICAL_AGGR)
                .addChildren(Pattern.create(OperatorType.LOGICAL_UNION)
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))
                        .addChildren(Pattern.create(OperatorType.PATTERN_LEAF))));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnablePushDownAggUnion()) {
            return false;
        }
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalUnionOperator union = (LogicalUnionOperator) input.inputAt(0).getOp();
        return agg.getType() == LOCAL && union.isUnionAll();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalAggregationOperator agg = (LogicalAggregationOperator) input.getOp();
        LogicalUnionOperator union = (LogicalUnionOperator) input.inputAt(0).getOp();
        List<OptExpression> aggs = new ArrayList<>();
        List<List<ColumnRefOperator>> unionInputs = new ArrayList<>();
        for (int i = 0; i < union.getChildOutputColumns().size(); i++) {
            int finalI = i;
            ScalarOperatorVisitor<ScalarOperator, Void> visitor = new ScalarOperatorVisitor<ScalarOperator, Void>() {
                @Override
                public ScalarOperator visit(ScalarOperator scalarOperator, Void context) {
                    List<ScalarOperator> children = Lists.newArrayList(scalarOperator.getChildren());
                    for (int i = 0; i < children.size(); ++i) {
                        scalarOperator.setChild(i, scalarOperator.getChild(i).accept(this, null));
                    }
                    return scalarOperator;
                }

                @Override
                public ScalarOperator visitVariableReference(ColumnRefOperator columnRefOperator, Void context) {
                    return union.getChildOutputColumns().get(finalI)
                            .get(union.getOutputColumnRefOp().indexOf(columnRefOperator));
                }
            };
            List<ScalarOperator> arguments = new ArrayList<>();
            Map<ColumnRefOperator, CallOperator> newAggMap = new HashMap<>();
            for (Map.Entry<ColumnRefOperator, CallOperator> entry : agg.getAggregations().entrySet()) {
                CallOperator newFn = (CallOperator) entry.getValue().clone();
                for (int index = 0; index < entry.getValue().getArguments().size(); index++) {
                    ScalarOperator argument = entry.getValue().getArguments().get(index);
                    if (argument.isConstantRef()) {
                        newFn.setChild(index, argument);
                    } else {
                        newFn.setChild(index, argument.accept(visitor, null));
                    }
                }
                newAggMap.put(entry.getKey(), newFn);

            }
            List<ColumnRefOperator> newGroupByKeys = new ArrayList<>(agg.getGroupingKeys().size());
            for (ColumnRefOperator groupByKey : agg.getGroupingKeys()) {
                newGroupByKeys.add((ColumnRefOperator) groupByKey.accept(visitor, null));
            }
            List<ColumnRefOperator> newPartitionKeys = new ArrayList<>(agg.getPartitionByColumns().size());
            for (ColumnRefOperator partitionKey : agg.getPartitionByColumns()) {
                newPartitionKeys.add((ColumnRefOperator) partitionKey.accept(visitor, null));
            }
            LogicalAggregationOperator newAgg = new LogicalAggregationOperator.Builder().withOperator(agg)
                    .setGroupingKeys(newGroupByKeys)
                    .setPartitionByColumns(newPartitionKeys)
                    .setAggregations(createNormalAgg(newAggMap))
                    .setPredicate(null)
                    .setLimit(Operator.DEFAULT_LIMIT)
                    .setProjection(null)
                    .build();
            OptExpression newAggExpr = OptExpression.create(newAgg, input.inputAt(0).inputAt(i));
            aggs.add(newAggExpr);

            ColumnRefSet columns = new ColumnRefSet();
            columns.union(newAgg.getGroupingKeys());
            columns.union(new ArrayList<>(agg.getAggregations().keySet()));

            unionInputs.add(columns.getColumnRefOperators(context.getColumnRefFactory()));
        }
        List<ColumnRefOperator> aggOutput = agg.getOutputColumns(null).getColumnRefOperators(context.getColumnRefFactory());
        LogicalUnionOperator newUnion = new LogicalUnionOperator(aggOutput, unionInputs, union.isUnionAll());
        return Lists.newArrayList(OptExpression.create(newUnion, aggs));
    }

    public Map<ColumnRefOperator, CallOperator> createNormalAgg(Map<ColumnRefOperator, CallOperator> aggregationMap) {
        Map<ColumnRefOperator, CallOperator> newAggregationMap = Maps.newHashMap();
        for (Map.Entry<ColumnRefOperator, CallOperator> entry : aggregationMap.entrySet()) {
            ColumnRefOperator column = entry.getKey();
            CallOperator aggregation = entry.getValue();

            CallOperator callOperator = new CallOperator(aggregation.getFnName(), aggregation.getType(),
                    aggregation.getChildren(), aggregation.getFunction(),
                    aggregation.isDistinct(), aggregation.isRemovedDistinct());

            newAggregationMap.put(
                    new ColumnRefOperator(column.getId(), column.getType(), column.getName(), column.isNullable()),
                    callOperator);
        }

        return newAggregationMap;
    }

}
