/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.cassandra;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.common.concurrent.PTExecutors;

public final class CassandraKeyValueServiceConfigManager {
    private static final Logger log = LoggerFactory.getLogger(CassandraKeyValueServiceConfigManager.class);

    private final Supplier<CassandraKeyValueServiceConfig> configSupplier;
    private final ScheduledExecutorService refreshExecutor;
    private final long initDelay;
    private final long refreshInterval;
    private CassandraKeyValueServiceConfig config;
    private boolean isShutdown = false;

    private static final long DEFAULT_INIT_DELAY = 1000 * 60;
    private static final long DEFAULT_REFRESH_INTERVAL = 1000 * 10;

    public static CassandraKeyValueServiceConfigManager createSimpleManager(CassandraKeyValueServiceConfig config) {
        CassandraKeyValueServiceConfigManager ret = new CassandraKeyValueServiceConfigManager(
                Suppliers.ofInstance(config),
                null,
                DEFAULT_INIT_DELAY,
                DEFAULT_REFRESH_INTERVAL);
        ret.init();
        return ret;
    }

    public static CassandraKeyValueServiceConfigManager create(
            Supplier<CassandraKeyValueServiceConfig> configSupplier) {
        return create(configSupplier, DEFAULT_INIT_DELAY, DEFAULT_REFRESH_INTERVAL);
    }

    public static CassandraKeyValueServiceConfigManager create(
            Supplier<CassandraKeyValueServiceConfig> configSupplier,
            long initDelay,
            long refreshInterval) {
        CassandraKeyValueServiceConfigManager ret = new CassandraKeyValueServiceConfigManager(
                configSupplier,
                PTExecutors.newScheduledThreadPool(1),
                initDelay,
                refreshInterval);
        ret.init();
        return ret;
    }

    /**
     * Refreshes the C* KVS config.
     *
     * @param configSupplier supplier that returns an up-to-date config
     * @param refreshExecutor disables polling updates when null
     * @param initDelay init delay on polling updates
     * @param refreshInterval refresh interval for polling updates
     */
    private CassandraKeyValueServiceConfigManager(
            Supplier<CassandraKeyValueServiceConfig> configSupplier,
            @Nullable ScheduledExecutorService refreshExecutor,
            long initDelay,
            long refreshInterval) {
        this.configSupplier = configSupplier;
        this.refreshExecutor = refreshExecutor;
        this.config = configSupplier.get();
        this.initDelay = initDelay;
        this.refreshInterval = refreshInterval;
    }

    private void init() {
        if (refreshExecutor != null) {
            refreshExecutor.scheduleWithFixedDelay(() -> {
                try {
                    config = configSupplier.get();
                }  catch (Throwable t) {
                    // If any execution of the task encounters an exception, subsequent executions are suppressed for
                    // ScheduledExecutorService.scheduleWithFixedDelay() we're catching Throwable (e.g.
                    // OutOfMemoryError, NPE, SocketException, Cassandra network error, etc.) to ensure the task
                    // doesn't get killed.
                    log.error("CassandraKeyValueServiceConfigManager encountered {}", t.toString(), t);
                }
            }, initDelay, refreshInterval, TimeUnit.MILLISECONDS);
        }
    }

    public CassandraKeyValueServiceConfig getConfig() {
        if (isShutdown) {
            throw new RuntimeException("CassandraKeyValueServiceConfig is shutdown");
        }
        return config;
    }

    public void shutdown() {
        isShutdown = true;
        if (refreshExecutor != null) {
            refreshExecutor.shutdown();
        }
    }
}
