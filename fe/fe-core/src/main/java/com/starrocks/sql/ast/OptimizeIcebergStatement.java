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

package com.starrocks.sql.ast;

import com.starrocks.analysis.Expr;
import com.starrocks.analysis.RedirectStatus;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Table;
import com.starrocks.sql.parser.NodePosition;

import java.util.Map;

public class OptimizeIcebergStatement extends StatementBase {
    private final TableName tableName;
    private final Expr wherePredicate;
    private final Map<String, String> properties;

    private Table table;
    private QueryStatement queryStatement;
    private InsertStmt insertStmt;

    public OptimizeIcebergStatement(TableName tableName,
                                    Expr wherePredicate,
                                    Map<String, String> properties) {
        this(tableName, wherePredicate, properties, NodePosition.ZERO);
    }

    public OptimizeIcebergStatement(TableName tableName,
                                    Expr wherePredicate,
                                    Map<String, String> properties,
                                    NodePosition pos) {
        super(pos);
        this.tableName = tableName;
        this.wherePredicate = wherePredicate;
        this.properties = properties;
    }

    public TableName getTableName() {
        return tableName;
    }

    public Expr getWherePredicate() {
        return wherePredicate;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public QueryStatement getQueryStatement() {
        return queryStatement;
    }

    public void setQueryStatement(QueryStatement queryStatement) {
        this.queryStatement = queryStatement;
    }

    public InsertStmt getInsertStmt() {
        return insertStmt;
    }

    public void setInsertStmt(InsertStmt insertStmt) {
        this.insertStmt = insertStmt;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitOptimizeIcebergStatement(this, context);
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }
}
