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

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.InstrumentedScheduledExecutorService;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.palantir.atlasdb.AtlasDbMetricNames;
import com.palantir.atlasdb.timelock.AsyncTimelockResource;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.AsyncTimelockServiceImpl;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.config.AsyncLockConfiguration;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.lock.NonTransactionalLockService;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.atlasdb.timelock.util.AsyncOrLegacyTimelockService;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.tritium.metrics.registry.MetricName;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import com.palantir.util.JavaSuppliers;

public class AsyncTimeLockServicesCreator implements TimeLockServicesCreator {
    private static final Logger log = LoggerFactory.getLogger(AsyncTimeLockServicesCreator.class);

    private final MetricsManager metricsManager;
    private final LockLog lockLog;
    private final PaxosLeadershipCreator leadershipCreator;
    private final AsyncLockConfiguration asyncLockConfiguration;

    public AsyncTimeLockServicesCreator(MetricsManager metricsManager,
            LockLog lockLog, PaxosLeadershipCreator leadershipCreator,
            AsyncLockConfiguration asyncLockConfiguration) {
        this.metricsManager = metricsManager;
        this.lockLog = lockLog;
        this.leadershipCreator = leadershipCreator;
        this.asyncLockConfiguration = asyncLockConfiguration;
    }

    @Override
    public TimeLockServices createTimeLockServices(
            String client,
            Supplier<ManagedTimestampService> rawTimestampServiceSupplier,
            Supplier<LockService> rawLockServiceSupplier) {
        log.info("Creating async timelock services for client {}", SafeArg.of("client", client));
        AsyncOrLegacyTimelockService asyncOrLegacyTimelockService;
        AsyncTimelockService asyncTimelockService = instrumentInLeadershipProxy(
                metricsManager.getTaggedRegistry(),
                AsyncTimelockService.class,
                () -> createRawAsyncTimelockService(client, rawTimestampServiceSupplier),
                client);
        asyncOrLegacyTimelockService = AsyncOrLegacyTimelockService.createFromAsyncTimelock(
                new AsyncTimelockResource(lockLog, asyncTimelockService));

        LockService lockService = instrumentInLeadershipProxy(
                metricsManager.getTaggedRegistry(),
                LockService.class,
                asyncLockConfiguration.disableLegacySafetyChecksWarningPotentialDataCorruption()
                        ? rawLockServiceSupplier
                        : JavaSuppliers.compose(NonTransactionalLockService::new, rawLockServiceSupplier),
                client);

        leadershipCreator.executeWhenLostLeadership(() ->
                metricsManager.deregisterTaggedMetrics(withTagIsCurrentSuspectedLeader(true)));

        leadershipCreator.executeWhenGainedLeadership(() ->
                metricsManager.deregisterTaggedMetrics(withTagIsCurrentSuspectedLeader(false)));

        return TimeLockServices.create(
                asyncTimelockService,
                lockService,
                asyncOrLegacyTimelockService,
                asyncTimelockService);
    }

    private static Predicate<MetricName> withTagIsCurrentSuspectedLeader(boolean currentLeader) {
        return metricName ->
                Optional.ofNullable(metricName.safeTags().get(AtlasDbMetricNames.TAG_CURRENT_SUSPECTED_LEADER))
                        .filter(x -> x.equals(String.valueOf(currentLeader)))
                        .isPresent();
    }

    private AsyncTimelockService createRawAsyncTimelockService(
            String client,
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
                AsyncLockService.createDefault(lockLog, reaperExecutor, timeoutExecutor),
                timestampServiceSupplier.get());
    }

    private <T> T instrumentInLeadershipProxy(TaggedMetricRegistry taggedMetrics, Class<T> serviceClass,
            Supplier<T> serviceSupplier, String client) {
        return instrument(taggedMetrics, serviceClass,
                leadershipCreator.wrapInLeadershipProxy(serviceSupplier, serviceClass), client);
    }

    private <T> T instrument(TaggedMetricRegistry metricRegistry, Class<T> serviceClass, T service, String client) {
        return AtlasDbMetrics.instrumentWithTaggedMetrics(
                metricRegistry, serviceClass, service, MetricRegistry.name(serviceClass),
                context -> ImmutableMap.of(
                        AtlasDbMetricNames.TAG_CLIENT, client,
                        AtlasDbMetricNames.TAG_CURRENT_SUSPECTED_LEADER, String.valueOf(
                                leadershipCreator.isCurrentSuspectedLeader())));
    }
}
