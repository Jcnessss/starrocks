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


package com.starrocks.connector.hive;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HiveMetaStoreTable;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.HiveView;
import com.starrocks.catalog.PartitionKey;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.connector.ConnectorTableId;
import com.starrocks.connector.MetastoreType;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.trino.TrinoViewColumnTypeConverter;
import com.starrocks.connector.trino.TrinoViewDefinition;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.AlterTableStmt;
import com.starrocks.sql.ast.CreateTableLikeStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.CreateViewStmt;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.MsckRepairTableStmt;
import com.starrocks.sql.ast.MsckRepairTableStmt.MsckRepairOp;
import com.starrocks.sql.ast.TableRenameClause;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.starrocks.connector.PartitionUtil.executeInNewThread;
import static com.starrocks.connector.hive.HiveWriteUtils.checkLocationProperties;
import static com.starrocks.connector.hive.HiveWriteUtils.createDirectory;
import static com.starrocks.connector.hive.HiveWriteUtils.isDirectory;
import static com.starrocks.connector.hive.HiveWriteUtils.isEmpty;
import static com.starrocks.connector.hive.HiveWriteUtils.listPartitions;
import static com.starrocks.connector.hive.HiveWriteUtils.pathExists;
import static com.starrocks.connector.hive.HiveWriteUtils.relativeLocation;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.toResourceName;

public class HiveMetastoreOperations {
    private static final Logger LOG = LogManager.getLogger(HiveMetastoreOperations.class);
    public static String BACKGROUND_THREAD_NAME_PREFIX = "background-get-partitions-statistics-";
    public static final String LOCATION_PROPERTY = "location";
    public static final String EXTERNAL_LOCATION_PROPERTY = "external_location";
    public static final String FILE_FORMAT = "file_format";
    private final CachingHiveMetastore metastore;
    private final boolean enableCatalogLevelCache;
    private final Configuration hadoopConf;
    private final MetastoreType metastoreType;
    private final String catalogName;

    public HiveMetastoreOperations(CachingHiveMetastore cachingHiveMetastore,
                                   boolean enableCatalogLevelCache,
                                   Configuration hadoopConf,
                                   MetastoreType metastoreType,
                                   String catalogName) {
        this.metastore = cachingHiveMetastore;
        this.enableCatalogLevelCache = enableCatalogLevelCache;
        this.hadoopConf = hadoopConf;
        this.metastoreType = metastoreType;
        this.catalogName = catalogName;
    }

    public List<String> getAllDatabaseNames() {
        return metastore.getAllDatabaseNames();
    }

    public void createDb(String dbName, Map<String, String> properties) {
        properties = properties == null ? new HashMap<>() : properties;
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.equalsIgnoreCase(LOCATION_PROPERTY)) {
                try {
                    URI uri = new Path(value).toUri();
                    FileSystem fileSystem = FileSystem.get(uri, hadoopConf);
                    fileSystem.exists(new Path(value));
                } catch (Exception e) {
                    LOG.error("Invalid location URI: {}", value, e);
                    throw new StarRocksConnectorException("Invalid location URI: %s. msg: %s", value, e.getMessage());
                }
            } else {
                throw new IllegalArgumentException("Unrecognized property: " + key);
            }
        }

        metastore.createDb(dbName, properties);
    }

    public void dropDb(String dbName, boolean force) throws MetaNotFoundException {
        Database database;
        try {
            database = getDb(dbName);
        } catch (Exception e) {
            LOG.error("Failed to access database {}", dbName, e);
            throw new MetaNotFoundException("Failed to access database " + dbName);
        }

        if (database == null) {
            throw new MetaNotFoundException("Not found database " + dbName);
        }

        String dbLocation = database.getLocation();
        if (Strings.isNullOrEmpty(dbLocation)) {
            throw new MetaNotFoundException("Database location is empty");
        }
        boolean deleteData = false;
        try {
            deleteData = !FileSystem.get(URI.create(dbLocation), hadoopConf)
                    .listLocatedStatus(new Path(dbLocation)).hasNext();
        } catch (Exception e) {
            LOG.error("Failed to check database directory", e);
        }

        metastore.dropDb(dbName, deleteData);
    }

    public Database getDb(String dbName) {
        return metastore.getDb(dbName);
    }

    public List<String> getAllTableNames(String dbName) {
        return metastore.getAllTableNames(dbName);
    }

    public boolean createTable(CreateTableStmt stmt, List<Column> partitionColumns) throws DdlException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTableName();
        Map<String, String> properties = stmt.getProperties() != null ? stmt.getProperties() : new HashMap<>();
        Path tablePath = null;
        boolean tableLocationExists = false;
        if (!stmt.isExternal()) {
            checkLocationProperties(properties);
            if (!Strings.isNullOrEmpty(properties.get(LOCATION_PROPERTY))) {
                String tableLocationWithUserAssign = properties.get(LOCATION_PROPERTY);
                tablePath = new Path(tableLocationWithUserAssign);
                if (pathExists(tablePath, hadoopConf)) {
                    tableLocationExists = true;
                    if (!isEmpty(tablePath, hadoopConf)) {
                        throw new StarRocksConnectorException("not support creating table under non-empty directory: %s",
                                tableLocationWithUserAssign);
                    }
                }
            } else {
                tablePath = getDefaultLocation(dbName, tableName);
            }
        } else {
            // checkExternalLocationProperties(properties);
            if (properties.containsKey(EXTERNAL_LOCATION_PROPERTY)) {
                tablePath = new Path(properties.get(EXTERNAL_LOCATION_PROPERTY));
            } else if (properties.containsKey(LOCATION_PROPERTY)) {
                tablePath = new Path(properties.get(LOCATION_PROPERTY));
            }
            tableLocationExists = true;
        }

        HiveStorageFormat.check(properties);

        List<String> partitionColNames;
        if (partitionColumns.isEmpty()) {
            partitionColNames = stmt.getPartitionDesc() != null ?
                    ((ListPartitionDesc) stmt.getPartitionDesc()).getPartitionColNames() : new ArrayList<>();
        } else {
            partitionColNames = partitionColumns.stream().map(Column::getName).collect(Collectors.toList());
        }

        // default is managed table
        HiveTable.HiveTableType tableType = HiveTable.HiveTableType.MANAGED_TABLE;
        if (stmt.isExternal()) {
            tableType = HiveTable.HiveTableType.EXTERNAL_TABLE;
        }
        HiveTable.Builder builder = HiveTable.builder()
                .setId(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt())
                .setTableName(tableName)
                .setCatalogName(catalogName)
                .setResourceName(toResourceName(catalogName, "hive"))
                .setHiveDbName(dbName)
                .setHiveTableName(tableName)
                .setPartitionColumnNames(partitionColNames)
                .setDataColumnNames(stmt.getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList()).subList(0, stmt.getColumns().size() - partitionColNames.size()))
                .setFullSchema(stmt.getColumns())
                .setTableLocation(tablePath == null ? null : tablePath.toString())
                .setProperties(stmt.getProperties())
                .setStorageFormat(HiveStorageFormat.get(properties.getOrDefault(FILE_FORMAT, "parquet")))
                .setCreateTime(System.currentTimeMillis())
                .setHiveTableType(tableType);
        Table table = builder.build();
        try {
            if (!tableLocationExists) {
                createDirectory(tablePath, hadoopConf);
            }
            metastore.createTable(dbName, table);
        } catch (Exception e) {
            LOG.error("Failed to create table {}.{}", dbName, tableName);
            boolean shouldDelete;
            try {
                if (tableExists(dbName, tableName)) {
                    LOG.warn("Table {}.{} already exists. But some error occur such as accessing meta service timeout",
                            dbName, table, e);
                    return true;
                }
                FileSystem fileSystem = FileSystem.get(URI.create(tablePath.toString()), hadoopConf);
                shouldDelete = !fileSystem.listLocatedStatus(tablePath).hasNext() && !tableLocationExists;
                if (shouldDelete) {
                    fileSystem.delete(tablePath);
                }
            } catch (Exception e1) {
                LOG.error("Failed to delete table location {}", tablePath, e);
            }
            throw new DdlException(String.format("Failed to create table %s.%s. msg: %s", dbName, tableName, e.getMessage()));
        }

        return true;
    }

    public boolean createTable(CreateTableStmt stmt) throws DdlException {
        return createTable(stmt, ImmutableList.of());
    }

    public boolean createTableLike(CreateTableLikeStmt stmt) throws DdlException {
        String existedDbName = stmt.getExistedDbName();
        String existedTableName = stmt.getExistedTableName();
        Table likeTable = getTable(existedDbName, existedTableName);
        return createTable(stmt.getCreateTableStmt(), likeTable.getPartitionColumns());
    }

    public void dropTable(String dbName, String tableName) {
        metastore.dropTable(dbName, tableName);
    }

    public Table getTable(String dbName, String tableName) {
        return metastore.getTable(dbName, tableName);
    }

    public boolean tableExists(String dbName, String tableName) {
        return metastore.tableExists(dbName, tableName);
    }

    public List<String> getPartitionKeys(String dbName, String tableName) {
        return metastore.getPartitionKeysByValue(dbName, tableName, HivePartitionValue.ALL_PARTITION_VALUES);
    }

    public List<String> getPartitionKeysByValue(String dbName, String tableName, List<Optional<String>> partitionValues) {
        return metastore.getPartitionKeysByValue(dbName, tableName, partitionValues);
    }

    public Partition getPartition(String dbName, String tableName, List<String> partitionValues) {
        return metastore.getPartition(dbName, tableName, partitionValues);
    }

    public void addPartitions(String dbName, String tableName, List<HivePartitionWithStats> partitions) {
        metastore.addPartitions(dbName, tableName, partitions);
    }

    public void dropPartition(String dbName, String tableName, List<String> partitionValues, boolean deleteData) {
        metastore.dropPartition(dbName, tableName, partitionValues, deleteData);
    }

    public boolean partitionExists(Table table, List<String> partitionValues) {
        return metastore.partitionExists(table, partitionValues);
    }

    public Map<String, Partition> getPartitionByPartitionKeys(Table table, List<PartitionKey> partitionKeys) {
        String dbName = ((HiveMetaStoreTable) table).getDbName();
        String tblName = ((HiveMetaStoreTable) table).getTableName();
        List<String> partitionColumnNames = ((HiveMetaStoreTable) table).getPartitionColumnNames();
        List<String> partitionNames = partitionKeys.stream()
                .map(partitionKey -> PartitionUtil.toHivePartitionName(partitionColumnNames, partitionKey))
                .collect(Collectors.toList());

        return metastore.getPartitionsByNames(dbName, tblName, partitionNames);
    }

    public Map<String, Partition> getPartitionByNames(Table table, List<String> partitionNames) {
        String dbName = ((HiveMetaStoreTable) table).getDbName();
        String tblName = ((HiveMetaStoreTable) table).getTableName();
        return metastore.getPartitionsByNames(dbName, tblName, partitionNames);
    }

    public Map<String, Partition> getPartitionByNames(String dbName, String tblName, List<String> partitionNames) {
        return metastore.getPartitionsByNames(dbName, tblName, partitionNames);
    }

    public HivePartitionStats getTableStatistics(String dbName, String tblName) {
        return metastore.getTableStatistics(dbName, tblName);
    }

    public Map<String, HivePartitionStats> getPartitionStatistics(Table table, List<String> partitionNames) {
        String catalogName = ((HiveMetaStoreTable) table).getCatalogName();
        String dbName = ((HiveMetaStoreTable) table).getDbName();
        String tblName = ((HiveMetaStoreTable) table).getTableName();
        List<HivePartitionName> hivePartitionNames = partitionNames.stream()
                .map(partitionName -> HivePartitionName.of(dbName, tblName, partitionName))
                .peek(hivePartitionName -> checkState(hivePartitionName.getPartitionNames().isPresent(),
                        "partition name is missing"))
                .collect(Collectors.toList());

        Map<String, HivePartitionStats> partitionStats;
        if (enableCatalogLevelCache) {
            partitionStats = metastore.getPresentPartitionsStatistics(hivePartitionNames);
            if (partitionStats.size() == partitionNames.size()) {
                return partitionStats;
            }

            String backgroundThreadName = String.format(BACKGROUND_THREAD_NAME_PREFIX + "%s-%s-%s",
                    catalogName, dbName, tblName);
            executeInNewThread(backgroundThreadName, () -> metastore.getPartitionStatistics(table, partitionNames));
        } else {
            partitionStats = metastore.getPartitionStatistics(table, partitionNames);
        }

        return partitionStats;
    }

    public void invalidateAll() {
        metastore.invalidateAll();
    }

    public void updateTableStatistics(String dbName, String tableName, Function<HivePartitionStats, HivePartitionStats> update) {
        metastore.updateTableStatistics(dbName, tableName, update);
    }

    public void updatePartitionStatistics(String dbName, String tableName, String partitionName,
                                          Function<HivePartitionStats, HivePartitionStats> update) {
        metastore.updatePartitionStatistics(dbName, tableName, partitionName, update);
    }

    public Path getDefaultLocation(String dbName, String tableName, boolean checkExists) {
        Database database = getDb(dbName);

        if (database == null) {
            throw new StarRocksConnectorException("Database '%s' not found", dbName);
        }
        if (Strings.isNullOrEmpty(database.getLocation())) {
            throw new StarRocksConnectorException("Failed to find location in database '%s'. Please define the location" +
                    " when you create table or recreate another database with location." +
                    " You could execute the SQL command like 'CREATE TABLE <table_name> <columns> " +
                    "PROPERTIES('location' = '<location>')", dbName);
        }

        String dbLocation = database.getLocation();
        Path databasePath = new Path(dbLocation);

        if (!pathExists(databasePath, hadoopConf)) {
            throw new StarRocksConnectorException("Database '%s' location does not exist: %s", dbName, databasePath);
        }

        if (!isDirectory(databasePath, hadoopConf)) {
            throw new StarRocksConnectorException("Database '%s' location is not a directory: %s",
                    dbName, databasePath);
        }

        Path targetPath = new Path(databasePath, tableName);
        if (checkExists) {
            if (pathExists(targetPath, hadoopConf)) {
                throw new StarRocksConnectorException("Target directory for table '%s.%s' already exists: %s",
                        dbName, tableName, targetPath);
            }
        }

        return targetPath;
    }

    public Path getDefaultLocation(String dbName, String tableName) {
        return getDefaultLocation(dbName, tableName, true);
    }

    public void createView(CreateViewStmt stmt) throws DdlException {
        SessionVariable sessionVariable = ConnectContext.get() != null ? ConnectContext.get().getSessionVariable()
                : new SessionVariable();
        String sqlDialect = sessionVariable.getSqlDialect();
        if (!sqlDialect.equals("trino")) {
            throw new DdlException("Please specify Trino SQL dialect");
        }

        List<Column> columns = stmt.getColumns();
        List<TrinoViewDefinition.ViewColumn> viewColumns = new ArrayList<>();
        for (Column column : columns) {
            TrinoViewDefinition.ViewColumn viewColumn = new TrinoViewDefinition.ViewColumn(
                    column.getName(),
                    TrinoViewColumnTypeConverter.toTrinoType(column.getType()),
                    column.getComment());
            viewColumns.add(viewColumn);
        }
        TrinoViewDefinition trinoViewDefinition = new TrinoViewDefinition(
                stmt.getInlineTrinoViewDef(),
                stmt.getTableName().getCatalog(),
                stmt.getDbName(),
                viewColumns,
                stmt.getComment(),
                null,
                true
        );

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(HiveView.TABLE_COMMENT, HiveView.PRESTO_VIEW_COMMENT)
                .put(HiveView.PRESTO_VIEW_FLAG, "true")
                .put(HiveView.TRINO_CREATED_BY, "Starrocks Hive connector")
                .buildOrThrow();

        Column dummyColumn = new Column("dummy", ScalarType.createVarcharType());

        String data = Base64.getEncoder().encodeToString(GsonUtils.GSON.toJson(trinoViewDefinition).getBytes());

        HiveTable.Builder builder = HiveTable.builder()
                .setId(ConnectorTableId.CONNECTOR_ID_GENERATOR.getNextId().asInt())
                .setTableName(stmt.getTable())
                .setCatalogName(catalogName)
                .setResourceName(toResourceName(catalogName, "hive"))
                .setHiveDbName(stmt.getDbName())
                .setHiveTableName(stmt.getTable())
                .setViewOriginalText(HiveView.PRESTO_VIEW_PREFIX + data + HiveView.PRESTO_VIEW_SUFFIX)
                .setViewExpandedText(HiveView.PRESTO_VIEW_EXPANDED_TEXT_MARKER)
                .setHiveTableType(HiveTable.HiveTableType.VIRTUAL_VIEW)
                .setProperties(properties)
                .setCreateTime(System.currentTimeMillis())
                .setPartitionColumnNames(ImmutableList.of())
                .setDataColumnNames(ImmutableList.of("dummy"))
                .setFullSchema(ImmutableList.of(dummyColumn))
                .setTableLocation(null)
                .setStorageFormat(null);
        Table table = builder.build();

        try {
            boolean isExists = tableExists(stmt.getDbName(), stmt.getTable());
            if (!isExists) {
                metastore.createTable(stmt.getDbName(), table);
            } else {
                Table oldTable = getTable(stmt.getDbName(), stmt.getTable());
                if (!stmt.isReplace()) {
                    throw new StarRocksConnectorException("Table '%s' already exists", stmt.getTable());
                }
                if (oldTable.isHiveView() && ((HiveView) oldTable).getViewType().equals(HiveView.Type.Trino)) {
                    LOG.info("view {} already exists, need to replace it", stmt.getTable());
                    metastore.alterTable(stmt.getDbName(), stmt.getTable(), table);
                    LOG.info("replace view {} successfully",  stmt.getTable());
                } else {
                    throw new StarRocksConnectorException("Table '%s' already exists, table type is '%s'",
                            stmt.getTable(), oldTable.getType().toString());
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new StarRocksConnectorException("Create view failed", e);
        }
    }

    private void checkRenameSource(TableName source) throws DdlException {
        Table sourceTable = getTable(source.getDb(), source.getTbl());
        if (sourceTable == null) {
            throw new DdlException(String.format("View '%s' does not exist", source));
        }
        if (!sourceTable.isHiveView()) {
            throw new DdlException(String.format("View '%s' does not exist, but a table with that name exists. " +
                            "Did you mean ALTER TABLE %s RENAME TO ...?", source, source));
        }
        if (!((HiveView)
                sourceTable).getViewType().equals(HiveView.Type.Trino)) {
            throw new DdlException(String.format("Trino View '%s' does not exist, " +
                    "but a hive view with that name exists", source));
        }
    }

    private void checkRenameTarget(TableName target) throws DdlException {
        boolean exists = tableExists(target.getDb(), target.getTbl());
        if (exists) {
            throw new DdlException(String.format("Target view '%s' already exists", target));
        }
    }

    private void renameView(AlterTableStmt stmt) throws DdlException {
        try {
            TableName source = stmt.getTbl();
            TableName target = ((TableRenameClause) stmt.getAlterClauseList().get(0)).getTrinoNewTableName();
            checkRenameSource(source);
            checkRenameTarget(target);

            org.apache.hadoop.hive.metastore.api.Table renameTable = metastore.getMetaStoreTable(
                    stmt.getTbl().getDb(), stmt.getTbl().getTbl());

            renameTable.setDbName(target.getDb());
            renameTable.setTableName(target.getTbl());

            metastore.alterTable(source.getDb(), source.getTbl(), renameTable);
        } catch (Exception e) {
            LOG.error(e.getMessage());
            throw new StarRocksConnectorException("Rename view failed", e);
        }
    }

    public void alterTable(AlterTableStmt stmt) throws DdlException {
        SessionVariable sessionVariable = ConnectContext.get() != null ? ConnectContext.get().getSessionVariable()
                : new SessionVariable();
        String sqlDialect = sessionVariable.getSqlDialect();
        if (!sqlDialect.equals("trino")) {
            throw new DdlException("Please specify Trino SQL dialect");
        }
        if (stmt.getAlterClauseList().size() > 1) {
            throw new DdlException("Hive catalog only support alter one operation");
        }

        switch (stmt.getAlterClauseList().get(0).getOpType()) {
            case RENAME:
                renameView(stmt);
                break;
            default:
                throw new DdlException("Hive catalog alter view type only support rename");
        }
    }

    public List<PartitionUpdate> msckRepairTable(MsckRepairTableStmt stmt, Table table) throws DdlException {
        final int BATCH_GET_PARTITIONS_BY_NAMES_MAX_PAGE_SIZE = 1000;

        String db = stmt.getTableName().getDb();
        String tbl = stmt.getTableName().getTbl();
        boolean isExist = tableExists(db, tbl);
        if (!isExist) {
            throw new DdlException(String.format("Table '%s' not found", stmt.getTableName()));
        }
        try {
            if (table.getPartitionColumns().isEmpty()) {
                throw new DdlException(String.format("Table is not partitioned: " + stmt.getTableName()));
            }

            MsckRepairOp op = stmt.getOp();
            boolean hasAdd = op == MsckRepairOp.ADD || op == MsckRepairOp.SYNC;
            boolean hasDrop = op == MsckRepairOp.DROP || op == MsckRepairOp.SYNC;

            Path tablePath;
            Map<String, String> properties = stmt.getProperties();
            if (!Strings.isNullOrEmpty(properties.get(LOCATION_PROPERTY))) {
                String location = properties.get(LOCATION_PROPERTY);
                tablePath = new Path(location);
                if (!pathExists(tablePath, hadoopConf)) {
                    throw new DdlException(String.format("Table location '%s' does not exist", tablePath));
                }
            } else {
                tablePath = getDefaultLocation(db, tbl, false);
            }
            Location tableLocation = Location.of(tablePath.toString());

            Set<String> partitionsToAdd = new HashSet<>();
            Set<String> partitionsToDrop = new HashSet<>();

            List<String> partitionsNamesInMetastore = getPartitionKeys(db, tbl);
            Set<String> abnormalRelativePartitionLocationsInMetastore = new HashSet<>();
            for (List<String> partitionsNamesBatch :
                    Lists.partition(partitionsNamesInMetastore, BATCH_GET_PARTITIONS_BY_NAMES_MAX_PAGE_SIZE)) {
                Map<String, Partition> map = metastore.getPartitionsByNames(db, tbl, partitionsNamesBatch);
                for (Map.Entry<String, Partition> entry : map.entrySet()) {
                    Partition partition = entry.getValue();
                    if (partition == null) {
                        continue;
                    }
                    String partitionName = entry.getKey();
                    Location partitionLocation = Location.of(partition.getFullPath());
                    if (hasAdd) {
                        Optional<String> optionalRelativeLocation = relativeLocation(tableLocation, partitionLocation);
                        if (optionalRelativeLocation.isPresent()) {
                            String relativeLocation = optionalRelativeLocation.get();
                            if (!partitionName.equals(relativeLocation)) {
                                abnormalRelativePartitionLocationsInMetastore.add(relativeLocation);
                            }
                        }
                    }
                    // partitions in metastore but not in file system
                    if (hasDrop) {
                        Path partitionPath = new Path(partition.getFullPath());
                        if (!pathExists(partitionPath, hadoopConf)) {
                            partitionsToDrop.add(partitionName);
                        }
                    }
                }
            }

            // partitions in file system but not in metastore
            if (hasAdd) {
                Set<String> partitionsInMetastore = ImmutableSet.copyOf(partitionsNamesInMetastore);
                Set<String> partitionsInFileSystem = listPartitions(tableLocation, table.getPartitionColumns(), hadoopConf);
                for (String relativizePath : partitionsInFileSystem) {
                    if (!partitionsInMetastore.contains(relativizePath) &&
                            !abnormalRelativePartitionLocationsInMetastore.contains(relativizePath)) {
                        partitionsToAdd.add(relativizePath);
                    }
                }
            }
            return syncPartitions(partitionsToAdd, partitionsToDrop, op, tableLocation);
        } catch (IOException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private List<PartitionUpdate> syncPartitions(Set<String> partitionsToAdd,
                                Set<String> partitionsToDrop,
                                MsckRepairOp op,
                                Location tableLocation) {
        List<PartitionUpdate> partitionUpdates = new ArrayList<>();
        if (op == MsckRepairOp.ADD || op == MsckRepairOp.SYNC) {
            partitionUpdates.addAll(partitionsToAdd.stream()
                    .map(partition -> addPartition(partition, tableLocation))
                    .collect(Collectors.toList()));
        }
        if (op == MsckRepairOp.DROP || op == MsckRepairOp.SYNC) {
            partitionUpdates.addAll(partitionsToDrop.stream()
                    .map(this::dropPartition)
                    .collect(Collectors.toList()));
        }
        return partitionUpdates;
    }

    private PartitionUpdate addPartition(String partition, Location tableLocation) {
        PartitionUpdate pu = new PartitionUpdate(
                partition,
                new Path(tableLocation.appendPath(partition).toString()),
                new Path(tableLocation.appendPath(partition).toString()),
                null,
                0,
                0);
        pu.setUpdateMode(PartitionUpdate.UpdateMode.NEW);
        return pu;
    }

    private PartitionUpdate dropPartition(String partition) {
        PartitionUpdate pu = new PartitionUpdate(
                partition,
                null,
                null,
                null,
                0,
                0);
        pu.setUpdateMode(PartitionUpdate.UpdateMode.DROP);
        pu.setDeleteData(false);
        return pu;
    }
}
