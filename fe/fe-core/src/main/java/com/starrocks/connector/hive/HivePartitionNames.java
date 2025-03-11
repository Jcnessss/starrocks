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

import java.util.List;
import java.util.OptionalLong;

public class HivePartitionNames {
    private final String databaseName;
    private final String tableName;
    private List<String> partitionNames;
    private OptionalLong version;

    public HivePartitionNames(String databaseName, String tableName, List<String> partitionNames, OptionalLong version) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.partitionNames = partitionNames;
        this.version = version;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(List<String> partitionNames) {
        this.partitionNames = partitionNames;
    }

    public OptionalLong getVersion() {
        return version;
    }

    public void setVersion(OptionalLong version) {
        this.version = version;
    }
}
