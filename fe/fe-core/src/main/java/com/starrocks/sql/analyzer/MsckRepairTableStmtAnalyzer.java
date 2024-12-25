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

import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.MsckRepairTableStmt;
import com.starrocks.sql.ast.StatementBase;

import java.util.logging.Logger;

public class MsckRepairTableStmtAnalyzer {
    private static final Logger LOG = Logger.getLogger(MsckRepairTableStmtAnalyzer.class.getName());

    public static void analyze(StatementBase stmt, ConnectContext session) {
        new MsckRepairTableStmtAnalyzerVisitor().visit(stmt, session);
    }

    static class MsckRepairTableStmtAnalyzerVisitor implements AstVisitor<Void, ConnectContext> {
        @Override
        public Void visitMsckRepairTableStatement(MsckRepairTableStmt stmt, ConnectContext context) {
            stmt.getTableName().normalization(context);
            final String tableName = stmt.getTableName().getTbl();
            FeNameFormat.checkTableName(tableName);
            return null;
        }
    }
}
