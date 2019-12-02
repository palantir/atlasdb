/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.lock.watch;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchRequest;
import com.palantir.logsafe.SafeArg;

public class LockWatchTestingService {
    private final Logger log = LoggerFactory.getLogger(LockWatchTestingService.class);
    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor();
    private final Supplier<LockWatchTestRuntimeConfig> runtime;
    private final Function<String, LockWatchingResource> resource;

    private LockWatchTestingService(
            Supplier<LockWatchTestRuntimeConfig> runtime,
            Function<String, LockWatchingResource> resource) {
        this.runtime = runtime;
        this.resource = resource;
    }

    public static void create(
            Supplier<LockWatchTestRuntimeConfig> runtime,
            Function<String, LockWatchingResource> resource) {
        LockWatchTestingService testService = new LockWatchTestingService(runtime, resource);
        testService.start();
    }

    private void start() {
        executor.scheduleAtFixedRate(this::runOneIteration, 5, 10, TimeUnit.SECONDS);
    }

    private void runOneIteration() {
        try {
            LockWatchTestRuntimeConfig config = runtime.get();
            if (config.namespaceToWatch().isPresent()) {
                LockWatchingResource lockWatcher = resource.apply(config.namespaceToWatch().get());
                LockWatchRequest request = LockWatchRequest.of(config.tablesToWatch().stream()
                        .map(TableReference::getQualifiedName)
                        .map(LockWatchReferences::entireTable)
                        .collect(Collectors.toSet()));
                long start = System.currentTimeMillis();
                lockWatcher.startWatching(request);
                log.info("Registered lock watches for keyspace {} and tables {} in {} seconds.",
                        SafeArg.of("keyspace", config.namespaceToWatch().get()),
                        SafeArg.of("tables", config.tablesToWatch()),
                        SafeArg.of("seconds", System.currentTimeMillis() - start));
            }
        } catch (Throwable th) {
            log.info("Failed to run a test iteration of registering lock watches", th);
        }
    }
}
