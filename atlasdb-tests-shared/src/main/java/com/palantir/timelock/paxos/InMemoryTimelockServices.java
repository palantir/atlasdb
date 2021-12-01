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

package com.palantir.timelock.paxos;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.LockWatchCachingConfig;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerImpl;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.timelock.AsyncTimelockResource;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.lock.LockService;
import com.palantir.lock.client.CommitTimestampGetter;
import com.palantir.lock.client.LeaderTimeGetter;
import com.palantir.lock.client.LegacyLeaderTimeGetter;
import com.palantir.lock.client.LockLeaseService;
import com.palantir.lock.client.LockWatchStarter;
import com.palantir.lock.client.NamespacedConjureTimelockService;
import com.palantir.lock.client.NamespacedConjureTimelockServiceImpl;
import com.palantir.lock.client.RequestBatchersFactory;
import com.palantir.lock.client.TransactionStarter;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.watch.LockWatchCache;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.refreshable.Refreshable;
import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.timelock.config.ClusterInstallConfiguration;
import com.palantir.timelock.config.ImmutableClusterInstallConfiguration;
import com.palantir.timelock.config.ImmutableDefaultClusterConfiguration;
import com.palantir.timelock.config.ImmutableSqlitePaxosPersistenceConfiguration;
import com.palantir.timelock.config.ImmutableTimeLockRuntimeConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.timestamp.ManagedTimestampService;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampService;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

public final class InMemoryTimelockServices extends ExternalResource implements TimeLockServices, Closeable {
    private static final String USER_AGENT_NAME = "user-agent";
    private static final String USER_AGENT_VERSION = "3.1415926.5358979";
    private static final UserAgent USER_AGENT = UserAgent.of(UserAgent.Agent.of(USER_AGENT_NAME, USER_AGENT_VERSION));
    private static final int THREAD_POOL_SIZE = 128;
    private static final int BLOCKING_TIMEOUT_MS = 60 * 800; // 0.8 mins to ms

    private final TemporaryFolder tempFolder;

    private String client;
    private TimeLockServices delegate;
    private TimeLockAgent timeLockAgent;

    private LockLeaseService lockLeaseService;
    private RequestBatchersFactory requestBatchersFactory;

    public InMemoryTimelockServices(TemporaryFolder tempFolder) {
        this.tempFolder = tempFolder;
        this.client = "client";
    }

    void setClient(String client) {
        this.client = client;
    }

    @Override
    protected void before() throws IOException {
        PaxosInstallConfiguration paxos = PaxosInstallConfiguration.builder()
                .dataDirectory(tryCreateSubFolder(tempFolder))
                .leaderMode(PaxosLeaderMode.SINGLE_LEADER)
                .sqlitePersistence(ImmutableSqlitePaxosPersistenceConfiguration.builder()
                        .dataDirectory(tryCreateSubFolder(tempFolder, client))
                        .build())
                .isNewService(false)
                .build();

        ClusterInstallConfiguration cluster = ImmutableClusterInstallConfiguration.builder()
                .enableNonstandardAndPossiblyDangerousTopology(true)
                .build();
        TimeLockInstallConfiguration install = TimeLockInstallConfiguration.builder()
                .paxos(paxos)
                .cluster(cluster)
                .build();

        ImmutableDefaultClusterConfiguration clusterConfig = ImmutableDefaultClusterConfiguration.builder()
                .localServer("local")
                .cluster(PartialServiceConfiguration.of(List.of("local"), Optional.empty()))
                .build();
        TimeLockRuntimeConfiguration runtime = ImmutableTimeLockRuntimeConfiguration.builder()
                .clusterSnapshot(clusterConfig)
                .build();

        timeLockAgent = TimeLockAgent.create(
                // TODO(gs): one MetricManager to rule them all
                MetricsManagers.of(new MetricRegistry(), SharedTaggedMetricRegistries.getSingleton()),
                install,
                Refreshable.only(runtime), // This won't actually live reload.
                runtime.clusterSnapshot(),
                USER_AGENT,
                THREAD_POOL_SIZE,
                BLOCKING_TIMEOUT_MS,
                _unused -> {},
                Optional.empty(),
                OrderableSlsVersion.valueOf("0.0.0"),
                ObjectMappers.newServerObjectMapper(),
                () -> System.exit(0));

        delegate = timeLockAgent.createInvalidatingTimeLockServices(client);

        // Create other stuff
        createHelperServices();

        // Wait for leadership
        Awaitility.await()
                .atMost(Duration.ofSeconds(30L))
                .pollInterval(Duration.ofMillis(50))
                .ignoreExceptions()
                .until(() -> delegate.getTimestampService().getFreshTimestamp() > 0);
    }

    // TODO(gs): unify
    private void createHelperServices() {
        MetricsManager metricsManager = MetricsManagers.createForTests();
        Set<Schema> schemas = ImmutableSet.of(); // TODO(gs): any schemas?
        LockWatchCachingConfig cachingConfig = LockWatchCachingConfig.builder().build();

        //        LeaderClock leaderClock = LeaderClock.create();
        //        HeldLocksCollection heldLocks = HeldLocksCollection.create(leaderClock);
        //        LeadershipId leadershipId = LeadershipId.random();
        //        LockWatchStarter lockWatchStarter = new LockWatchingServiceImpl(heldLocks, leadershipId);
        LockWatchStarter lockWatchStarter = delegate.getTimelockService();
        LockWatchManagerInternal lockWatchManager =
                LockWatchManagerImpl.create(metricsManager, schemas, lockWatchStarter, cachingConfig);

        LockWatchCache lockWatchCache = lockWatchManager.getCache();
        requestBatchersFactory = RequestBatchersFactory.create(lockWatchCache, Namespace.of(client), Optional.empty());

        ConjureTimelockService cts = timeLockAgent.getConjureTimeLockService();
        NamespacedConjureTimelockService ncts = new NamespacedConjureTimelockServiceImpl(cts, client);
        LeaderTimeGetter ltg = new LegacyLeaderTimeGetter(ncts);
        lockLeaseService = LockLeaseService.create(ncts, ltg);
    }

    @Override
    @After
    public void after() {
        close();
    }

    @Override
    public void close() {
        timeLockAgent.shutdown();
    }

    public static InMemoryTimelockServices create(TemporaryFolder tempFolder) {
        return create(tempFolder, "client");
    }

    public static InMemoryTimelockServices create(TemporaryFolder tempFolder, String client) {
        InMemoryTimelockServices services = new InMemoryTimelockServices(tempFolder);
        services.setClient(client);

        try {
            services.before();
        } catch (IOException e) {
            throw new SafeRuntimeException("Failed to create InMemoryTimelockServices", e);
        }

        return services;
    }

    private static File tryCreateSubFolder(TemporaryFolder tempFolder) {
        try {
            return tempFolder.newFolder();
        } catch (IOException e) {
            throw new SafeRuntimeException("Failed to create temporary folder", e);
        }
    }

    private static File tryCreateSubFolder(TemporaryFolder tempFolder, String subFolderName) {
        try {
            return tempFolder.newFolder(subFolderName);
        } catch (IOException e) {
            throw new SafeRuntimeException("Failed to create temporary folder", e);
        }
    }

    @Override
    public TimestampService getTimestampService() {
        return delegate.getTimestampService();
    }

    @Override
    public LockService getLockService() {
        return delegate.getLockService();
    }

    @Override
    public AsyncTimelockResource getTimelockResource() {
        return delegate.getTimelockResource();
    }

    @Override
    public AsyncTimelockService getTimelockService() {
        return delegate.getTimelockService();
    }

    @Override
    public TimestampManagementService getTimestampManagementService() {
        return delegate.getTimestampManagementService();
    }

    public ManagedTimestampService getManagedTimestampService() {
        return delegate.getTimelockService();
    }

    public TimelockService getLegacyTimelockService() {
        TransactionStarter transactionStarter = TransactionStarter.create(lockLeaseService, requestBatchersFactory);
        CommitTimestampGetter commitTimestampGetter =
                requestBatchersFactory.createBatchingCommitTimestampGetter(lockLeaseService);
        return new DelegatingTimelockService(
                transactionStarter, lockLeaseService, getTimelockService(), commitTimestampGetter);
    }
}
