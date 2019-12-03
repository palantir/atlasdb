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

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventVisitor;
import com.palantir.lock.watch.LockWatchOpenLocksEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchRequest;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.SafeArg;

public class LockWatchTestingService {
    private static final Logger log = LoggerFactory.getLogger(LockWatchTestingService.class);
    private final ScheduledExecutorService executor = PTExecutors.newSingleThreadScheduledExecutor();
    private final Supplier<LockWatchTestRuntimeConfig> runtime;
    private final Function<String, LockWatchingResource> resource;
    private final OpenLocksFilter filter = new OpenLocksFilter();

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
        executor.scheduleAtFixedRate(this::runOneIteration, 5, 5, TimeUnit.MINUTES);
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
                LockWatchStateUpdate versionBefore = lockWatcher.getWatchState(
                        OptionalLong.empty());

                long startRegistering = System.currentTimeMillis();
                lockWatcher.startWatching(request);
                log.info("Registered lock watches for keyspace {} and tables {} in {} milliseconds.",
                        SafeArg.of("keyspace", config.namespaceToWatch().get()),
                        SafeArg.of("tables", config.tablesToWatch()),
                        SafeArg.of("seconds", System.currentTimeMillis() - startRegistering));

                long startUpdate = System.currentTimeMillis();
                LockWatchStateUpdate versionAfter = lockWatcher.getWatchState(versionBefore.lastKnownVersion());
                long duration = System.currentTimeMillis() - startUpdate;
                if (versionAfter.success()) {
                    List<Integer> result = versionAfter.events().stream()
                            .map(event -> event.accept(filter))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .map(LockWatchOpenLocksEvent::lockDescriptors)
                            .map(Set::size)
                            .collect(Collectors.toList());
                    if (result.isEmpty()) {
                        log.info("Registered lock watches, but did not find any open locks. Took {} milliseconds",
                                SafeArg.of("duration", duration));
                    } else {
                        log.info("Registered lock watches, found {} open locks entries in the log. They contained {}"
                                        + " locks respecively. This registration event ook {} milliseconds.",
                                SafeArg.of("numberOfEvents", result.size()),
                                SafeArg.of("numberOfWatches", result),
                                SafeArg.of("duration", duration));
                    }
                } else {
                    log.info("Registered lock watches, but was unable to get an update. Last known version is {}",
                            SafeArg.of("lastKnownVersion", versionAfter.lastKnownVersion()));
                }

            }
        } catch (Throwable th) {
            log.info("Failed to run a test iteration of registering lock watches", th);
        }
    }

    private static class OpenLocksFilter implements LockWatchEventVisitor<Optional<LockWatchOpenLocksEvent>> {
        @Override
        public Optional<LockWatchOpenLocksEvent> visit(LockEvent lockEvent) {
            return Optional.empty();
        }

        @Override
        public Optional<LockWatchOpenLocksEvent> visit(UnlockEvent unlockEvent) {
            return Optional.empty();
        }

        @Override
        public Optional<LockWatchOpenLocksEvent> visit(LockWatchOpenLocksEvent openLocksEvent) {
            return Optional.of(openLocksEvent);
        }

        @Override
        public Optional<LockWatchOpenLocksEvent> visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return Optional.empty();
        }
    }
}
