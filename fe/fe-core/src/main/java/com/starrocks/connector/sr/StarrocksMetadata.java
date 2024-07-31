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

package com.starrocks.connector.sr;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.StarrocksTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.rpc.FrontendServiceProxy;
import com.starrocks.service.FrontendOptions;
import com.starrocks.sql.common.MetaNotFoundException;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.ColumnStatistic;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TColumnDef;
import com.starrocks.thrift.TDescribeTableParams;
import com.starrocks.thrift.TDescribeTableResult;
import com.starrocks.thrift.TGetDbsParams;
import com.starrocks.thrift.TGetDbsResult;
import com.starrocks.thrift.TGetTableMetaRequest;
import com.starrocks.thrift.TGetTableMetaResponse;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TGetTablesParams;
import com.starrocks.thrift.TGetTablesResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TStatusCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;


public class StarrocksMetadata implements ConnectorMetadata {
    private static final Logger LOG = LogManager.getLogger(StarrocksMetadata.class);

    private final String catalogName;
    private final String remoteFeHost;
    private final String remoteFeRpcPort;
    private final String remoteFeHttpPort;
    private final String remoteFeUsername;
    private final String remoteFePasswd;

    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Database> databases = new ConcurrentHashMap<>();

    public StarrocksMetadata(String catalogName, String remoteFeHost,
                             String remoteFeRpcPort, String remoteFeHttpPort,
                             String remoteFeUsername, String remoteFePasswd) {
        this.catalogName = catalogName;
        this.remoteFeHost = remoteFeHost;
        this.remoteFeRpcPort = remoteFeRpcPort;
        this.remoteFeHttpPort = remoteFeHttpPort;
        this.remoteFeUsername = remoteFeUsername;
        this.remoteFePasswd = remoteFePasswd;
    }

    @Override
    public List<String> listDbNames() {
        TNetworkAddress addr = new TNetworkAddress(remoteFeHost, Integer.parseInt(remoteFeRpcPort));
        TGetDbsParams request = new TGetDbsParams();
        request.setUser(remoteFeUsername);
        request.setUser_ip(FrontendOptions.getLocalHostAddress());
        try {
            TGetDbsResult response = FrontendServiceProxy.call(addr,
                    Config.thrift_rpc_timeout_ms,
                    Config.thrift_rpc_retry_times,
                    client -> client.getDbNames(request));
            if (response.dbs.isEmpty()) {
                final String errMsg = "get dbs from remote starrocks cluster failed or no db found.";
                LOG.warn(errMsg);
                throw new MetaNotFoundException(errMsg);
            } else {
                return ImmutableList.copyOf(response.dbs);
            }
        } catch (Exception e) {
            LOG.warn("call fe {} getDbNames rpc method failed", addr, e);
            throw new MetaNotFoundException("getDbNames failed from " + addr + ", error: " + e.getMessage());
        }
    }

    @Override
    public List<String> listTableNames(String dbName) {
        TNetworkAddress addr = new TNetworkAddress(remoteFeHost, Integer.parseInt(remoteFeRpcPort));
        TGetTablesParams request = new TGetTablesParams();
        request.setDb(dbName);
        request.setUser(remoteFeUsername);
        request.setUser_ip(FrontendOptions.getLocalHostAddress());
        try {
            TGetTablesResult response = FrontendServiceProxy.call(addr,
                    Config.thrift_rpc_timeout_ms,
                    Config.thrift_rpc_retry_times,
                    client -> client.getTableNames(request));
            if (response.tables.isEmpty()) {
                final String errMsg = "get tables from remote starrocks cluster failed or no tables found.";
                LOG.warn(errMsg);
                throw new MetaNotFoundException(errMsg);
            } else {
                return ImmutableList.copyOf(response.tables);
            }
        } catch (Exception e) {
            LOG.warn("call fe {} getTableNames rpc method failed", addr, e);
            throw new MetaNotFoundException("getTableNames failed from " + addr + ", error: " + e.getMessage());
        }
    }

    @Override
    public Database getDb(String dbName) {
        List<String> dbs = listDbNames();
        if (dbs.stream().anyMatch(db -> dbName.equals(db))) {
            return databases.computeIfAbsent(dbName, d -> new Database(CONNECTOR_ID_GENERATOR.getNextId().asInt(), d));
        }
        LOG.error("Starrocks database {}.{} done not exist.", catalogName, dbName);
        return null;
    }

    @Override
    public Table getTable(String dbName, String tblName) {
        List<String> tableNames = listTableNames(dbName);
        ArrayList<Column> columns = new ArrayList<>();
        if (tableNames.stream().anyMatch(table -> tblName.equals(table))) {
            return tables.computeIfAbsent(tblName, tableName -> {
                TNetworkAddress addr = new TNetworkAddress(remoteFeHost, Integer.parseInt(remoteFeRpcPort));
                TDescribeTableParams request = new TDescribeTableParams();
                request.setDb(dbName);
                request.setTable_name(tblName);
                request.setUser(remoteFeUsername);
                request.setUser_ip(FrontendOptions.getLocalHostAddress());
                try {
                    TDescribeTableResult response = FrontendServiceProxy.call(addr,
                            Config.thrift_rpc_timeout_ms,
                            Config.thrift_rpc_retry_times,
                            client -> client.describeTable(request));
                    if (response.columns.isEmpty()) {
                        final String errMsg = "describe tables from remote starrocks cluster failed or no tables found.";
                        LOG.warn(errMsg);
                        throw new MetaNotFoundException(errMsg);
                    } else {
                        for (int i = 0; i < response.columns.size(); i++) {
                            TColumnDef columnSchema = response.columns.get(i);
                            String fieldName = columnSchema.getColumnDesc().columnName;
                            boolean isKey = columnSchema.getColumnDesc().isKey();
                            String comment = columnSchema.getComment();
                            Type fieldType = Type.fromPrimitiveType(
                                    PrimitiveType.fromThrift(columnSchema.getColumnDesc().getColumnType()));
                            Column column = new Column(fieldName, fieldType, isKey,
                                    null, columnSchema.getColumnDesc().allowNull, null, comment);
                            columns.add(column);
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("call fe {} getTableNames rpc method failed", addr, e);
                    throw new MetaNotFoundException("getTableNames failed from " + addr + ", error: " + e.getMessage());
                }
                TGetTableMetaRequest metaRequest = new TGetTableMetaRequest();
                metaRequest.setDb_name(dbName);
                metaRequest.setTable_name(tableName);
                try {
                    TGetTableMetaResponse response = FrontendServiceProxy.call(addr,
                            Config.thrift_rpc_timeout_ms,
                            Config.thrift_rpc_retry_times,
                            client -> client.getTableMeta(metaRequest));
                    if (response.status.getStatus_code() != TStatusCode.OK) {
                        String errMsg;
                        if (response.status.getError_msgs() != null) {
                            errMsg = String.join(",", response.status.getError_msgs());
                        } else {
                            errMsg = "";
                        }
                        LOG.warn("get TableMeta failed: {}", errMsg);
                        throw new MetaNotFoundException(errMsg);
                    } else {
                        List<String> partitionColumns = new ArrayList<>();
                        if (response.getTable_meta().getPartition_info().type == TPartitionType.RANGE_PARTITIONED) {
                            partitionColumns = response.getTable_meta().getPartition_info().getRange_partition_desc()
                                    .getColumns().stream().map(desc -> desc.columnName).collect(Collectors.toList());
                        }
                        return new StarrocksTable(remoteFeHost, remoteFeHttpPort, remoteFeUsername, remoteFePasswd,
                                catalogName, dbName, tblName, columns, partitionColumns);
                    }
                } catch (Exception e) {
                    LOG.warn("call fe {} refreshTable rpc method failed", addr, e);
                    throw new MetaNotFoundException("get TableMeta failed from " + addr + ", error: " + e.getMessage());
                }
            });
        }
        LOG.error("Starrocks table {}.{} does not exist.", dbName, tblName);
        return null;
    }

    @Override
    public Table.TableType getTableType() {
        return Table.TableType.STARROCKS;
    }

    @Override
    public Statistics getTableStatistics(OptimizerContext session,
                                         Table table,
                                         Map<ColumnRefOperator, Column> columns,
                                         List<PartitionKey> partitionKeys,
                                         ScalarOperator predicate,
                                         long limit) {
        TNetworkAddress addr = new TNetworkAddress(remoteFeHost, Integer.parseInt(remoteFeRpcPort));
        TGetTablesInfoRequest request = new TGetTablesInfoRequest();
        TAuthInfo tAuthInfo = new TAuthInfo();
        tAuthInfo.setUser(remoteFeUsername);
        tAuthInfo.setUser_ip(FrontendOptions.getLocalHostAddress());
        request.setAuth_info(tAuthInfo);
        request.setTable_name(table.getName());
        try {
            TGetTablesInfoResponse response = FrontendServiceProxy.call(addr,
                    Config.thrift_rpc_timeout_ms,
                    Config.thrift_rpc_retry_times,
                    client -> client.getTablesInfo(request));
            if (response.tables_infos.isEmpty()) {
                final String errMsg = "getTablesInfo from remote starrocks cluster failed or no table found.";
                LOG.warn(errMsg);
                throw new MetaNotFoundException(errMsg);
            } else {
                Statistics.Builder builder = Statistics.builder();
                for (ColumnRefOperator columnRefOperator : columns.keySet()) {
                    builder.addColumnStatistic(columnRefOperator, ColumnStatistic.unknown());
                }
                builder.setOutputRowCount(response.tables_infos.get(0).table_rows);
                return builder.build();
            }
        } catch (Exception e) {
            LOG.warn("call fe {} getTablesInfo rpc method failed", addr, e);
            throw new MetaNotFoundException("getTablesInfo failed from " + addr + ", error: " + e.getMessage());
        }
    }
}
