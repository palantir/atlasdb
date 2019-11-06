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
package com.palantir.timelock.paxos;

import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.InstrumentedScheduledExecutorService;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.timelock.AsyncTimelockResource;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.AsyncTimelockServiceImpl;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.config.TargetedSweepLockControlConfig;
import com.palantir.atlasdb.timelock.config.TargetedSweepLockControlConfig.RateLimitConfig;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingResource;
import com.palantir.atlasdb.timelock.lock.watch.LockWatchingService;
import com.palantir.atlasdb.timelock.lock.LockWatchingServiceImpl;
import com.palantir.atlasdb.timelock.lock.NonTransactionalLockService;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.timestamp.ManagedTimestampService;

public class AsyncTimeLockServicesCreator implements TimeLockServicesCreator {
    private static final Logger log = LoggerFactory.getLogger(AsyncTimeLockServicesCreator.class);

    private final MetricsManager metricsManager;
    private final LockLog lockLog;
    private final PaxosLeadershipCreator leadershipCreator;
    private final Supplier<TargetedSweepLockControlConfig> lockControlConfigSupplier;

    public AsyncTimeLockServicesCreator(
            MetricsManager metricsManager,
            LockLog lockLog,
            PaxosLeadershipCreator leadershipCreator,
            Supplier<TargetedSweepLockControlConfig> lockControlConfigSupplier) {
        this.metricsManager = metricsManager;
        this.lockLog = lockLog;
        this.leadershipCreator = leadershipCreator;
        this.lockControlConfigSupplier = lockControlConfigSupplier;
    }

    @Override
    public TimeLockServices createTimeLockServices(
            String client,
            Supplier<ManagedTimestampService> rawTimestampServiceSupplier,
            Supplier<LockService> rawLockServiceSupplier) {
        log.info("Creating async timelock services for client {}", SafeArg.of("client", client));

        LockWatchingService lockWatchingService = leadershipCreator.wrapInLeadershipProxy(
                () -> new LockWatchingServiceImpl(rawTimestampServiceSupplier.get()::getFreshTimestamp),
                LockWatchingService.class,
                client);
        LockWatchingResource lockWatchingResource = new LockWatchingResource(lockWatchingService);

        AsyncTimelockService asyncTimelockService = leadershipCreator.wrapInLeadershipProxy(
                () -> createRawAsyncTimelockService(client, lockWatchingService, rawTimestampServiceSupplier),
                AsyncTimelockService.class,
                client);
        AsyncTimelockResource asyncTimelockResource = new AsyncTimelockResource(lockLog, asyncTimelockService);


        LockService lockService = leadershipCreator.wrapInLeadershipProxy(
                Suppliers.compose(NonTransactionalLockService::new, rawLockServiceSupplier::get),
                LockService.class,
                client);

        return TimeLockServices.create(
                asyncTimelockService,
                lockService,
                asyncTimelockResource,
                lockWatchingResource,
                asyncTimelockService);
    }

    private AsyncTimelockService createRawAsyncTimelockService(
            String client,
            LockWatchingService lockWatchingService,
            Supplier<ManagedTimestampService> timestampServiceSupplier) {
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
                        lockLog,
                        reaperExecutor,
                        timeoutExecutor,
                        rateLimitConfig(client),
                        lockWatchingService),
                timestampServiceSupplier.get());
    }

    private Supplier<RateLimitConfig> rateLimitConfig(String client) {
        return () -> lockControlConfigSupplier.get().rateLimitConfig(client);
    }

}
