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

package com.starrocks.catalog;

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

public class StarrocksTable extends Table {

    private String remoteFeHost;
    private String remoteFePort;
    private String remoteFeUsername;

    private String remoteFePasswd;
    private String catalogName;
    private String databaseName;
    private String tableName;
    private List<String> partColNames;
    private Map<String, String> properties;
    public StarrocksTable() {
        super(TableType.STARROCKS);
    }

    public StarrocksTable(String remoteFeHost, String remoteFePort, String remoteFeUsername,
                          String remoteFePasswd, String catalogName, String dbName,
                          String tblName, List<Column> schema, List<String> partColNames) {
        super(CONNECTOR_ID_GENERATOR.getNextId().asInt(), tblName, TableType.STARROCKS, schema);
        this.remoteFeHost = remoteFeHost;
        this.remoteFePort = remoteFePort;
        this.remoteFeUsername = remoteFeUsername;
        this.remoteFePasswd = remoteFePasswd;
        this.catalogName = catalogName;
        this.databaseName = dbName;
        this.tableName = tblName;
        this.partColNames = partColNames;
    }

    @Override
    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getRemoteFeHost() {
        return remoteFeHost;
    }

    public String getRemoteFePort() {
        return remoteFePort;
    }

    public String getRemoteFeUsername() {
        return remoteFeUsername;
    }

    public String getRemoteFePasswd() {
        return remoteFePasswd;
    }
    @Override
    public List<Column> getPartitionColumns() {
        List<Column> partitionColumns = new ArrayList<>();
        if (!partColNames.isEmpty()) {
            partitionColumns = partColNames.stream().map(this::getColumn)
                    .collect(Collectors.toList());
        }
        return partitionColumns;
    }

    @Override
    public Map<String, String> getProperties() {
        if (properties == null) {
            this.properties = new HashMap<>();
        }
        return properties;
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partColNames;
    }

    @Override
    public boolean isPartitioned() {
        return !partColNames.isEmpty();
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        return new TTableDescriptor(id, TTableType.STARROCKS_TABLE,
                fullSchema.size(), 0, tableName, databaseName);
    }
}
