/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.factory;

import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class AtlasDbRuntimeConfigRefreshable implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AtlasDbRuntimeConfigRefreshable.class);
    private static final Duration REFRESH_INTERVAL = Duration.ofSeconds(1);
    private static final Duration GRACEFUL_SHUTDOWN = Duration.ofSeconds(5);
    private static final AtlasDbRuntimeConfig DEFAULT_RUNTIME = AtlasDbRuntimeConfig.defaultRuntimeConfig();

    private final Refreshable<AtlasDbRuntimeConfig> delegate;
    private final Runnable closer;

    private AtlasDbRuntimeConfigRefreshable(
            Refreshable<Optional<AtlasDbRuntimeConfig>> delegate,
            Runnable closer) {
        this.delegate = delegate.map(config -> config.orElse(DEFAULT_RUNTIME));
        this.closer = closer;
    }

    public Refreshable<AtlasDbRuntimeConfig> config() {
        return delegate;
    }

    @Override
    public void close() {
        closer.run();
    }

    static AtlasDbRuntimeConfigRefreshable create(TransactionManagers builder) {
        return builder.runtimeConfig()
                .map(AtlasDbRuntimeConfigRefreshable::wrap)
                .orElseGet(() -> {
                    Supplier<Optional<AtlasDbRuntimeConfig>> runtimeConfig = builder.runtimeConfigSupplier()
                            .orElse(Optional::empty);
                    return AtlasDbRuntimeConfigRefreshable.createPolling(runtimeConfig);
                });
    }

    private static AtlasDbRuntimeConfigRefreshable wrap(Refreshable<Optional<AtlasDbRuntimeConfig>> delegate) {
        return new AtlasDbRuntimeConfigRefreshable(delegate, () -> {
        });
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    private static AtlasDbRuntimeConfigRefreshable createPolling(Supplier<Optional<AtlasDbRuntimeConfig>> config) {
        SettableRefreshable<Optional<AtlasDbRuntimeConfig>> refreshable = Refreshable.create(call(config));

        ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor();
        executor.scheduleWithFixedDelay(
                () -> {
                    try {
                        refreshable.update(config.get());
                    } catch (Throwable e) {
                        // This should not occur in practice
                        log.error("Failed to reload runtime config", e);
                    }
                },
                REFRESH_INTERVAL.toNanos(),
                REFRESH_INTERVAL.toNanos(),
                TimeUnit.NANOSECONDS);

        return new AtlasDbRuntimeConfigRefreshable(refreshable, () -> {
            if (!MoreExecutors.shutdownAndAwaitTermination(
                    executor, GRACEFUL_SHUTDOWN.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Executor did not terminate within graceful shutdown duration");
            }
        });
    }

    private static <T> T call(Supplier<T> callable) {
        try {
            return callable.get();
        } catch (Throwable e) {
            throw new SafeRuntimeException("Cannot create Refreshable unless initial value is constructable", e);
        }
    }
}
