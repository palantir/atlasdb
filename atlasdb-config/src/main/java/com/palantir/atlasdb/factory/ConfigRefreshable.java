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


import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.refreshable.DefaultRefreshable;
import com.palantir.refreshable.Refreshable;

final class ConfigRefreshable implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConfigRefreshable.class);
    private static final Duration REFRESH_INTERVAL = Duration.ofSeconds(1);

    private final Refreshable<Optional<AtlasDbRuntimeConfig>> delegate;
    private final Runnable closer;

    private ConfigRefreshable(
            Refreshable<Optional<AtlasDbRuntimeConfig>> delegate,
            Runnable closer) {
        this.delegate = delegate;
        this.closer = closer;
    }

    public Refreshable<Optional<AtlasDbRuntimeConfig>> refreshable() {
        return delegate;
    }

    @Override
    public void close() {
        closer.run();
    }

    static ConfigRefreshable wrap(Refreshable<Optional<AtlasDbRuntimeConfig>> delegate) {
        return new ConfigRefreshable(delegate, () -> {});
    }

    @SuppressWarnings("FutureReturnValueIgnored")
    static ConfigRefreshable createPolling(Supplier<Optional<AtlasDbRuntimeConfig>> config) {
        DefaultRefreshable<Optional<AtlasDbRuntimeConfig>> refreshable = new DefaultRefreshable<>(call(config));

        ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor();
        executor.scheduleWithFixedDelay(
                () -> {
                    try {
                        refreshable.update(config.get());
                    } catch (Throwable e) {
                        // TODO(jkozlowski): This is rather bad if it occurs,
                        // ideally we would propagate and not just throw our hands in the air
                        log.error("Failed to load runtime config", e);
                    }
                },
                REFRESH_INTERVAL.toNanos(),
                REFRESH_INTERVAL.toNanos(),
                TimeUnit.NANOSECONDS);

        return new ConfigRefreshable(refreshable, executor::shutdown);
    }

    private static <T> T call(Supplier<T> callable) {
        try {
            return callable.get();
        } catch (Throwable e) {
            throw new SafeRuntimeException("Cannot create Refreshable unless initial value is constructable", e);
        }
    }
}
