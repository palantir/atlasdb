/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
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
import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.timelock.AsyncTimelockResource;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.AsyncTimelockServiceImpl;
import com.palantir.atlasdb.timelock.SecureTimelockResource;
import com.palantir.atlasdb.timelock.SecureTimelockService;
import com.palantir.atlasdb.timelock.SecureTimelockServiceImpl;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.config.AsyncLockConfiguration;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.NonTransactionalLockService;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.util.AsyncOrLegacyTimelockService;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.JavaSuppliers;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;

public class AsyncTimeLockServicesCreator implements TimeLockServicesCreator {
    private static final Logger log = LoggerFactory.getLogger(AsyncTimeLockServicesCreator.class);

    private final PaxosLeadershipCreator leadershipCreator;
    private final AsyncLockConfiguration asyncLockConfiguration;
    private final Supplier<TimeLockRuntimeConfiguration> runtime;

    public AsyncTimeLockServicesCreator(PaxosLeadershipCreator leadershipCreator,
            AsyncLockConfiguration asyncLockConfiguration,
            Supplier<TimeLockRuntimeConfiguration> runtime) {
        this.leadershipCreator = leadershipCreator;
        this.asyncLockConfiguration = asyncLockConfiguration;
        this.runtime = runtime;
    }

    @Override
    public TimeLockServices createTimeLockServices(
            String client,
            Supplier<ManagedTimestampService> rawTimestampServiceSupplier,
            Supplier<LockService> rawLockServiceSupplier) {
        log.info("Creating async timelock services for client {}", SafeArg.of("client", client));
        AsyncOrLegacyTimelockService asyncOrLegacyTimelockService;

        LockService lockService = instrumentInLeadershipProxy(
                LockService.class,
                asyncLockConfiguration.disableLegacySafetyChecksWarningPotentialDataCorruption()
                        ? rawLockServiceSupplier
                        : JavaSuppliers.compose(NonTransactionalLockService::new, rawLockServiceSupplier),
                client);

        if (runtime.get().clientTokens().containsKey(client)) {
            SecureTimelockService secureTimelockService = instrumentInLeadershipProxy(
                    SecureTimelockService.class,
                    () -> AsyncTimeLockServicesCreator.createRawSecureTimelockService(client,
                            rawTimestampServiceSupplier),
                    client);
            asyncOrLegacyTimelockService = AsyncOrLegacyTimelockService.createFromSecureTimelock(
                    new SecureTimelockResource(secureTimelockService));

            return TimeLockServices.create(
                    secureTimelockService,
                    lockService,
                    asyncOrLegacyTimelockService,
                    secureTimelockService);
        }

        AsyncTimelockService asyncTimelockService = instrumentInLeadershipProxy(
                AsyncTimelockService.class,
                () -> AsyncTimeLockServicesCreator.createRawAsyncTimelockService(client,
                        rawTimestampServiceSupplier),
                client);
        asyncOrLegacyTimelockService = AsyncOrLegacyTimelockService.createFromAsyncTimelock(
                new AsyncTimelockResource(asyncTimelockService));

        return TimeLockServices.create(
                asyncTimelockService,
                lockService,
                asyncOrLegacyTimelockService,
                asyncTimelockService);
    }

    private static SecureTimelockService createRawSecureTimelockService(
            String client,
            Supplier<ManagedTimestampService> timestampServiceSupplier) {
        return new SecureTimelockServiceImpl(createRawAsyncTimelockService(client, timestampServiceSupplier));
    }

    private static AsyncTimelockService createRawAsyncTimelockService(String client,
            Supplier<ManagedTimestampService> timestampServiceSupplier) {
        ScheduledExecutorService reaperExecutor = new InstrumentedScheduledExecutorService(
                PTExecutors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                        .setNameFormat("async-lock-reaper-" + client + "-%d")
                        .setDaemon(true)
                        .build()), AtlasDbMetrics.getMetricRegistry(), "async-lock-reaper");
        ScheduledExecutorService timeoutExecutor = new InstrumentedScheduledExecutorService(
                PTExecutors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                        .setNameFormat("async-lock-timeouts-" + client + "-%d")
                        .setDaemon(true)
                        .build()), AtlasDbMetrics.getMetricRegistry(), "async-lock-timeouts");
        return new AsyncTimelockServiceImpl(
                AsyncLockService.createDefault(reaperExecutor, timeoutExecutor),
                timestampServiceSupplier.get());
    }

    private <T> T instrumentInLeadershipProxy(Class<T> serviceClass, Supplier<T> serviceSupplier, String client) {
        return instrument(serviceClass, leadershipCreator.wrapInLeadershipProxy(serviceSupplier, serviceClass), client);
    }

    private static <T> T instrument(Class<T> serviceClass, T service, String client) {
        // TODO(nziebart): tag with the client name, when tritium supports it
        return AtlasDbMetrics.instrument(serviceClass, service, MetricRegistry.name(serviceClass));
    }
}
