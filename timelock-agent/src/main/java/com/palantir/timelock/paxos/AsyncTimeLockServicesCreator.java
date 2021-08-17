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

import com.codahale.metrics.InstrumentedScheduledExecutorService;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.debug.LockDiagnosticConfig;
import com.palantir.atlasdb.timelock.AsyncTimelockResource;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.AsyncTimelockServiceImpl;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lock.NonTransactionalLockService;
import com.palantir.atlasdb.timelock.paxos.LeadershipComponents;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.Client;
import com.palantir.timestamp.ManagedTimestampService;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

public class AsyncTimeLockServicesCreator implements TimeLockServicesCreator {
    private static final SafeLogger log = SafeLoggerFactory.get(AsyncTimeLockServicesCreator.class);

    private final MetricsManager metricsManager;
    private final LockLog lockLog;
    private final LeadershipComponents leadershipComponents;
    private final Map<Client, LockDiagnosticConfig> lockDiagnosticConfig;

    AsyncTimeLockServicesCreator(
            MetricsManager metricsManager,
            LockLog lockLog,
            LeadershipComponents leadershipComponents,
            // TODO(fdesouza): Remove this once PDS-95791 is resolved.
            Map<Client, LockDiagnosticConfig> lockDiagnosticConfig) {
        this.metricsManager = metricsManager;
        this.lockLog = lockLog;
        this.leadershipComponents = leadershipComponents;
        this.lockDiagnosticConfig = lockDiagnosticConfig;
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

        LockService lockService = leadershipComponents.wrapInLeadershipProxy(
                client,
                LockService.class,
                Suppliers.compose(NonTransactionalLockService::new, rawLockServiceSupplier::get));

        leadershipComponents.registerClientForLeaderElectionHealthCheck(client);

        return TimeLockServices.create(
                asyncTimelockService, lockService, asyncTimelockService, asyncTimelockResource, asyncTimelockService,
                leadershipComponents::renewRenewables);
    }

    private AsyncTimelockService createRawAsyncTimelockService(
            Client client, Supplier<ManagedTimestampService> timestampServiceSupplier, LockLog maybeEnhancedLockLog) {
        ScheduledExecutorService reaperExecutor = new InstrumentedScheduledExecutorService(
                PTExecutors.newSingleThreadScheduledExecutor(
                        new NamedThreadFactory("async-lock-reaper-" + client, true)),
                metricsManager.getRegistry(),
                "async-lock-reaper");
        ScheduledExecutorService timeoutExecutor = new InstrumentedScheduledExecutorService(
                PTExecutors.newSingleThreadScheduledExecutor(
                        new NamedThreadFactory("async-lock-timeouts-" + client, true)),
                metricsManager.getRegistry(),
                "async-lock-timeouts");
        return new AsyncTimelockServiceImpl(
                AsyncLockService.createDefault(maybeEnhancedLockLog, reaperExecutor, timeoutExecutor),
                timestampServiceSupplier.get(),
                maybeEnhancedLockLog);
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
