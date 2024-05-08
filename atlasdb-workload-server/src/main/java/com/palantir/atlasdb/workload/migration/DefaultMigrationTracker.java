/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.migration;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.awaitility.Awaitility;

public class DefaultMigrationTracker implements MigrationTracker {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultMigrationTracker.class);
    private final Set<String> keyspaceMigrationTracker = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean isMigrationComplete = new AtomicBoolean(false);

    @Override
    public void markRebuildAsStarted(String keyspace) {
        log.info("Marking rebuild as started for keyspace {}", SafeArg.of("keyspace", keyspace));
        keyspaceMigrationTracker.add(keyspace);
    }

    @Override
    public void markMigrationAsComplete() {
        log.info("Marking migration as complete");
        isMigrationComplete.set(true);
    }

    @Override
    public void blockOnRebuildStarting(String keyspace) {
        log.info("Blocking on rebuild starting or migration terminating");
        waitUntil(() -> isMigrationComplete.get() || keyspaceMigrationTracker.contains(keyspace));
    }

    @Override
    public void blockOnMigrationCompleting() {
        log.info("Blocking on migration completing");
        waitUntil(isMigrationComplete::get);
    }

    private void waitUntil(BooleanSupplier predicate) {
        if (predicate.getAsBoolean()) { // So we don't need to wait 100ms when the migration has already finished
            return;
        }
        long startTime = System.currentTimeMillis();
        Awaitility.await() // Horrible abuses of awaitility, but it's quick and hacky
                .atMost(Duration.ofMinutes(10))
                .pollInterval(Duration.ofMillis(100))
                .until(predicate::getAsBoolean);
        long finishTime = System.currentTimeMillis(); // Could be context switched, but ah well
        log.info("Waited for predicate for {} ms", SafeArg.of("duration", finishTime - startTime));
    }
}
