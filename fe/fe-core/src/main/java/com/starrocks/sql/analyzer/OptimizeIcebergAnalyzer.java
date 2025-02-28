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

package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Table;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.OptimizeIcebergStatement;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.SelectList;
import com.starrocks.sql.ast.SelectListItem;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.TableRelation;
import com.starrocks.sql.common.MetaUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OptimizeIcebergAnalyzer {

    private static final String PREDICATE_CHECK = "predicate_check";
    private static final String FILE_SIZE_THRESHOLD = "file_size_threshold";
    private static final String MAX_PARTITION_DATA_SIZE = "max_partition_data_size";
    private static final String WRITE_TABLE_LOCATION = "write_table_location";
    private static final String WRITE_COLUMNS = "write_columns";

    public static void analyze(OptimizeIcebergStatement stmt, ConnectContext session) {
        TableName tableName = stmt.getTableName();
        MetaUtils.normalizationTableName(session, tableName);
        MetaUtils.getDatabase(session, tableName);
        Table table = MetaUtils.getTable(session, tableName);

        if (!(table instanceof IcebergTable)) {
            throw new SemanticException("Optimize only support Iceberg tables");
        }

        Map<String, String> properties = handleDefaultProperties(stmt.getProperties(), session);

        String cols = properties.getOrDefault(WRITE_COLUMNS, null);
        List<String> targetColumnNames = null;
        if (cols != null) {
            targetColumnNames = Lists.newArrayList(cols.split(","));
        }

        SelectList selectList = new SelectList();
        List<Column> assignColumnList = Lists.newArrayList();
        Collection<Column> columnsToProcess = targetColumnNames != null
                ? targetColumnNames.stream()
                .map(colName -> {
                    Column col = table.getColumn(colName);
                    if (col == null) {
                        throw new SemanticException("Column " + colName + " not found");
                    }
                    return col;
                }).collect(Collectors.toList())
                : table.getBaseSchema();

        for (Column col : columnsToProcess) {
            String colName = col.getName();
            SelectListItem item = new SelectListItem(new SlotRef(tableName, colName), colName);
            selectList.addItem(item);
            assignColumnList.add(col);
        }

        TableRelation relation = new TableRelation(tableName);
        relation.setOptimizeProperties(properties);
        SelectRelation selectRelation =
                new SelectRelation(selectList, relation, stmt.getWherePredicate(), null, null);

        QueryStatement queryStatement = new QueryStatement(selectRelation);
        queryStatement.setIsExplain(stmt.isExplain(), stmt.getExplainLevel());
        new QueryAnalyzer(session).analyze(queryStatement);

        stmt.setTable(table);
        stmt.setQueryStatement(queryStatement);

        List<Expr> outputExpression = queryStatement.getQueryRelation().getOutputExpression();
        Preconditions.checkState(outputExpression.size() == assignColumnList.size());

        InsertStmt insertStmt = new InsertStmt(tableName, queryStatement, stmt.getProperties(), targetColumnNames,
                stmt.getPos());
        stmt.setInsertStmt(insertStmt);
    }

    private static Map<String, String> handleDefaultProperties(Map<String, String> properties, ConnectContext session) {
        if (!properties.containsKey(PREDICATE_CHECK)) {
            properties.put(PREDICATE_CHECK, "1");
        }

        if (!properties.containsKey(FILE_SIZE_THRESHOLD)) {
            properties.put(FILE_SIZE_THRESHOLD,
                    String.valueOf(session.getSessionVariable().getOptimizeFileSizeThreshold()));
        }

        return properties;
    }
}
