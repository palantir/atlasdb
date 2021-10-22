/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.config.TimeLockRequestBatcherProviders;
import com.palantir.atlasdb.keyvalue.api.LockWatchCachingConfig;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerImpl;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.client.CommitTimestampGetter;
import com.palantir.lock.client.ImmutableMultiClientRequestBatchers;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import com.palantir.lock.client.LeaderTimeGetter;
import com.palantir.lock.client.LockLeaseService;
import com.palantir.lock.client.NamespacedConjureTimeLockServiceFactory;
import com.palantir.lock.client.NamespacedConjureTimelockService;
import com.palantir.lock.client.RequestBatchersFactory;
import com.palantir.lock.client.metrics.TimeLockFeedbackBackgroundTask;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.lock.watch.LockWatchStarter;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

// A factory class for sub-services in the TimeLock ecosystem
public final class TimeAndLockServices {
    private final NamespacedConjureTimelockService namespacedConjureTimelockService;
    private final CommitTimestampGetter commitTimestampGetter;
    private final LockLeaseService lockLeaseService;
    private final LockWatchManagerInternal lockWatchManager;
    private final RequestBatchersFactory requestBatchersFactory;

    private TimeAndLockServices(
            NamespacedConjureTimelockService namespacedConjureTimelockService,
            CommitTimestampGetter commitTimestampGetter,
            LockLeaseService lockLeaseService,
            LockWatchManagerInternal lockWatchManager,
            RequestBatchersFactory requestBatchersFactory) {
        this.namespacedConjureTimelockService = namespacedConjureTimelockService;
        this.commitTimestampGetter = commitTimestampGetter;
        this.lockLeaseService = lockLeaseService;
        this.lockWatchManager = lockWatchManager;
        this.requestBatchersFactory = requestBatchersFactory;
    }

    public NamespacedConjureTimelockService namespacedConjureTimelockService() {
        return namespacedConjureTimelockService;
    }

    public CommitTimestampGetter commitTimestampGetter() {
        return commitTimestampGetter;
    }

    public LockLeaseService lockLeaseService() {
        return lockLeaseService;
    }

    public LockWatchManagerInternal lockWatchManager() {
        return lockWatchManager;
    }

    public RequestBatchersFactory requestBatchersFactory() {
        return requestBatchersFactory;
    }

    public static TimeAndLockServices create(
            String client,
            ConjureTimelockService conjureTimelockService,
            LockWatchStarter lockWatchStarter,
            LeaderTimeFactory leaderTimeFactory) {
        return create(
                client,
                conjureTimelockService,
                lockWatchStarter,
                LockWatchCachingConfig.builder().build(),
                ImmutableSet.of(),
                MetricsManagers.createForTests(),
                leaderTimeFactory,
                Optional.empty(),
                Optional.empty(),
                () -> null);
    }

    public static TimeAndLockServices create(
            String timelockNamespace,
            ConjureTimelockService conjureTimelockService,
            LockWatchStarter lockWatchingService,
            LockWatchCachingConfig cachingConfig,
            Set<Schema> schemas,
            MetricsManager metricsManager,
            LeaderTimeFactory leaderTimeFactory,
            Optional<TimeLockFeedbackBackgroundTask> timeLockFeedbackBackgroundTask,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier) {
        NamespacedConjureTimelockService namespacedConjureTimelockService =
                NamespacedConjureTimeLockServiceFactory.create(
                        conjureTimelockService,
                        timelockNamespace,
                        timeLockFeedbackBackgroundTask,
                        metricsManager.getTaggedRegistry());

        LockWatchManagerInternal lockWatchManager =
                LockWatchManagerImpl.create(metricsManager, schemas, lockWatchingService, cachingConfig);

        RequestBatchersFactory requestBatchersFactory = getRequestBatchersFactory(
                timelockNamespace,
                timelockRequestBatcherProviders,
                lockWatchManager.getCache(),
                multiClientTimelockServiceSupplier);

        LeaderTimeGetter leaderTimeGetter =
                getLeaderTimeGetter(timelockNamespace, namespacedConjureTimelockService, leaderTimeFactory);
        LockLeaseService lockLeaseService = LockLeaseService.create(namespacedConjureTimelockService, leaderTimeGetter);

        CommitTimestampGetter commitTimestampGetter =
                requestBatchersFactory.createBatchingCommitTimestampGetter(lockLeaseService);

        return new TimeAndLockServices(
                namespacedConjureTimelockService,
                commitTimestampGetter,
                lockLeaseService,
                lockWatchManager,
                requestBatchersFactory);
    }

    private static RequestBatchersFactory getRequestBatchersFactory(
            String namespace,
            Optional<TimeLockRequestBatcherProviders> timelockRequestBatcherProviders,
            LockWatchCache lockWatchCache,
            Supplier<InternalMultiClientConjureTimelockService> multiClientTimelockServiceSupplier) {
        return RequestBatchersFactory.create(
                lockWatchCache,
                Namespace.of(namespace),
                timelockRequestBatcherProviders.map(batcherProviders -> ImmutableMultiClientRequestBatchers.of(
                        batcherProviders.commitTimestamps().getBatcher(multiClientTimelockServiceSupplier),
                        batcherProviders.startTransactions().getBatcher(multiClientTimelockServiceSupplier))));
    }

    private static LeaderTimeGetter getLeaderTimeGetter(
            String timelockNamespace,
            NamespacedConjureTimelockService namespacedConjureTimelockService,
            LeaderTimeFactory leaderTimeFactory) {
        return leaderTimeFactory.leaderTimeGetter(timelockNamespace, namespacedConjureTimelockService);
    }
}
