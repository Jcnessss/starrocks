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

package com.starrocks.connector.iceberg.lock;


import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.iceberg.Table;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;

public class IcebergTableLock {
    private static final String ROOT = "/iceberg_table_locks";

    private final Cache<String, Locker> lockCache = Caffeine.newBuilder()
            .expireAfterAccess(10, TimeUnit.MINUTES)
            .build();

    private final Optional<CuratorFramework> client;
    private final int zookeeperLockSemaphorePermits;

    public IcebergTableLock(Optional<String> zookeeperConnectionString, int zookeeperLockSemaphorePermits) {
        client = zookeeperConnectionString.map(connectionString -> {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            CuratorFramework client = CuratorFrameworkFactory.builder()
                    .namespace("distributedlock")
                    .connectString(connectionString)
                    .retryPolicy(retryPolicy)
                    .build();
            client.start();
            return client;
        });
        checkArgument(zookeeperLockSemaphorePermits > 0,
                "zookeeperLockSemaphorePermits must be greater than 0");
        this.zookeeperLockSemaphorePermits = zookeeperLockSemaphorePermits;
    }

    public Locker obtain(Table table) {
        return lockCache.get(table.name(), k -> {
            if (client.isPresent()) {
                String path = ROOT + "/" + k;
                return new ZKLock(client.get(), path, zookeeperLockSemaphorePermits);
            } else {
                return new LocalLock();
            }
        });
    }
}
