/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.services;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import dagger.Module;
import dagger.Provides;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.inject.Named;
import javax.inject.Singleton;

@Module
public class RawKeyValueServiceModule {
    private static final SafeLogger log = SafeLoggerFactory.get(RawKeyValueServiceModule.class);
    private static final int DELAY_BETWEEN_INITIALIZATION_CHECKS_MILLIS = 100;

    @Provides
    @Singleton
    @Named("rawKvs")
    public KeyValueService provideRawKeyValueService(ServicesConfig config, MetricsManager metricsManager) {
        return config.atlasDbSupplier(metricsManager).getKeyValueService();
    }

    @Provides
    @Singleton
    @Named("initializedRawKvs")
    public KeyValueService provideInitializedRawKeyValueService(@Named("rawKvs") KeyValueService rawKeyValueService) {
        return transformToFuture(rawKeyValueService).join();
    }

    private CompletableFuture<KeyValueService> transformToFuture(KeyValueService keyValueService) {
        CompletableFuture<KeyValueService> future = new CompletableFuture<>();
        ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor();
        ScheduledFuture<?> scheduledFuture = executor.scheduleAtFixedRate(
                () -> {
                    try {
                        if (keyValueService.isInitialized()) {
                            future.complete(keyValueService);
                        } else {
                            log.info("Waiting for KeyValueService to initialize");
                        }
                    } catch (Exception e) {
                        future.completeExceptionally(e);
                    }
                },
                0,
                DELAY_BETWEEN_INITIALIZATION_CHECKS_MILLIS,
                TimeUnit.MILLISECONDS);
        future.whenComplete((_result, _thrown) -> {
            scheduledFuture.cancel(true);
            executor.shutdown();
        });
        return future;
    }
}
