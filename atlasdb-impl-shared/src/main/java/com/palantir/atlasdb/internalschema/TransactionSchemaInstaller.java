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

package com.palantir.atlasdb.internalschema;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.SafeArg;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TransactionSchemaInstaller implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(TransactionSchemaInstaller.class);

    @VisibleForTesting
    static final Duration POLLING_INTERVAL = Duration.ofMinutes(10);

    private final TransactionSchemaManager manager;
    private final Supplier<Optional<Integer>> versionToInstall;
    private final ScheduledExecutorService scheduledExecutorService;

    private TransactionSchemaInstaller(
            TransactionSchemaManager manager,
            Supplier<Optional<Integer>> versionToInstall,
            ScheduledExecutorService scheduledExecutorService) {
        this.manager = manager;
        this.versionToInstall = versionToInstall;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public static TransactionSchemaInstaller createStarted(
            TransactionSchemaManager manager,
            Supplier<Optional<Integer>> versionToInstall) {
        ScheduledExecutorService scheduledExecutor = PTExecutors.newSingleThreadScheduledExecutor(
                PTExecutors.newNamedThreadFactory(true));
        return createStarted(manager, versionToInstall, scheduledExecutor);
    }

    @VisibleForTesting
    static TransactionSchemaInstaller createStarted(
            TransactionSchemaManager manager,
            Supplier<Optional<Integer>> versionToInstall,
            ScheduledExecutorService scheduledExecutor) {
        TransactionSchemaInstaller installer = new TransactionSchemaInstaller(
                manager, versionToInstall, scheduledExecutor);
        scheduledExecutor.scheduleAtFixedRate(
                installer::runOneIteration, 0, POLLING_INTERVAL.toMinutes(), TimeUnit.MINUTES);
        return installer;

    }

    private void runOneIteration() {
        try {
            runOneIterationUnsafe();
        } catch (Exception e) {
            log.info("Encountered an error when trying to install a new transactions schema version."
                            + " This is probably benign and we will retry, but pending version changes may be delayed.",
                    e);
        }
    }

    private void runOneIterationUnsafe() {
        Optional<Integer> version = versionToInstall.get();
        version.ifPresent(presentVersion -> {
            if (!manager.tryInstallNewTransactionsSchemaVersion(presentVersion)) {
                log.info("We attempted to install transactions schema version {} because we saw it in configuration, "
                                + " but this was unsuccessful because another service changed the database. This is"
                                + " probably benign, but note that such version changes may be delayed.",
                        SafeArg.of("version", presentVersion));
            }
        });
    }

    @Override
    public void close() {
        scheduledExecutorService.shutdown();
    }
}
