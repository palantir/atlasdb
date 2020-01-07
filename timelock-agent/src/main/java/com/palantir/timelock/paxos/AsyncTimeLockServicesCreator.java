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
package com.palantir.timelock.paxos;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.InstrumentedScheduledExecutorService;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.debug.LockDiagnosticConfig;
import com.palantir.atlasdb.timelock.AsyncTimelockResource;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.AsyncTimelockServiceImpl;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.config.TargetedSweepLockControlConfig;
import com.palantir.atlasdb.timelock.config.TargetedSweepLockControlConfig.RateLimitConfig;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lock.NonTransactionalLockService;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingResource;
import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.atlasdb.timelock.paxos.LeadershipComponents;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.timestamp.ManagedTimestampService;

public class AsyncTimeLockServicesCreator implements TimeLockServicesCreator {
    private static final Logger log = LoggerFactory.getLogger(AsyncTimeLockServicesCreator.class);

    private final MetricsManager metricsManager;
    private final LockLog lockLog;
    private final LeadershipComponents leadershipComponents;
    private final Map<Client, LockDiagnosticConfig> lockDiagnosticConfig;
    private final Supplier<TargetedSweepLockControlConfig> lockControlConfigSupplier;

    AsyncTimeLockServicesCreator(
            MetricsManager metricsManager,
            LockLog lockLog,
            LeadershipComponents leadershipComponents,
            // TODO(fdesouza): Remove this once PDS-95791 is resolved.
            Map<Client, LockDiagnosticConfig> lockDiagnosticConfig,
            Supplier<TargetedSweepLockControlConfig> lockControlConfigSupplier) {
        this.metricsManager = metricsManager;
        this.lockLog = lockLog;
        this.leadershipComponents = leadershipComponents;
        this.lockDiagnosticConfig = lockDiagnosticConfig;
        this.lockControlConfigSupplier = lockControlConfigSupplier;
    }

    @Override
    public TimeLockServices createTimeLockServices(
            Client client,
            Supplier<ManagedTimestampService> rawTimestampServiceSupplier,
            Supplier<LockService> rawLockServiceSupplier) {
        log.info("Creating async timelock services for client {}", SafeArg.of("client", client));
        LockLog maybeEnhancedLockLog = maybeEnhancedLockLog(client);

        AsyncTimelockService asyncTimelockService = leadershipComponents.wrapInLeadershipProxy(
                client,
                AsyncTimelockService.class,
                () -> createRawAsyncTimelockService(client, rawTimestampServiceSupplier, maybeEnhancedLockLog));

        AsyncTimelockResource asyncTimelockResource =
                new AsyncTimelockResource(maybeEnhancedLockLog, asyncTimelockService);
        LockWatchingResource lockWatchingResource = new LockWatchingResource(asyncTimelockService);

        LockService lockService = leadershipComponents.wrapInLeadershipProxy(
                client,
                LockService.class,
                Suppliers.compose(NonTransactionalLockService::new, rawLockServiceSupplier::get));

        return TimeLockServices.create(
                asyncTimelockService,
                lockService,
                asyncTimelockResource,
                lockWatchingResource,
                asyncTimelockService);
    }

    private AsyncTimelockService createRawAsyncTimelockService(
            Client client,
            Supplier<ManagedTimestampService> timestampServiceSupplier,
            LockLog maybeEnhancedLockLog) {
        ScheduledExecutorService reaperExecutor = new InstrumentedScheduledExecutorService(
                PTExecutors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                        .setNameFormat("async-lock-reaper-" + client + "-%d")
                        .setDaemon(true)
                        .build()), metricsManager.getRegistry(), "async-lock-reaper");
        ScheduledExecutorService timeoutExecutor = new InstrumentedScheduledExecutorService(
                PTExecutors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                        .setNameFormat("async-lock-timeouts-" + client + "-%d")
                        .setDaemon(true)
                        .build()), metricsManager.getRegistry(), "async-lock-timeouts");
        return new AsyncTimelockServiceImpl(
                AsyncLockService.createDefault(
                        maybeEnhancedLockLog,
                        reaperExecutor,
                        timeoutExecutor,
                        rateLimitConfig(client)),
                timestampServiceSupplier.get());
    }

    private Supplier<RateLimitConfig> rateLimitConfig(Client client) {
        return () -> lockControlConfigSupplier.get().rateLimitConfig(client.value());
    }

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    private LockLog maybeEnhancedLockLog(Client client) {
        return Optional.ofNullable(lockDiagnosticConfig.get(client))
                .map(lockLog::withLockRequestDiagnosticCollection)
                .orElse(lockLog);
    }

}
