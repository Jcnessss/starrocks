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

import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;

import java.util.concurrent.Semaphore;

import static com.google.common.base.Preconditions.checkArgument;

public class ZKLock implements Locker {
    private final Semaphore localSemaphore;
    private final InterProcessMutex interProcessMutex;

    public ZKLock(CuratorFramework client, String path, int semaphorePermits) {
        interProcessMutex = new InterProcessMutex(client, path);
        checkArgument(semaphorePermits > 0, "semaphorePermits must be greater than 0");
        localSemaphore = new Semaphore(semaphorePermits, true);
    }

    @Override
    public void withLock(Runnable runnable) {
        try {
            localSemaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        try {
            withInterProcessMutex(runnable);
        } catch (Exception e) {
            throw new StarRocksConnectorException(e.getMessage(), e);
        } finally {
            localSemaphore.release();
        }
    }

    private void withInterProcessMutex(Runnable runnable) throws Exception {
        interProcessMutex.acquire();
        try {
            runnable.run();
        } finally {
            interProcessMutex.release();
        }
    }
}
