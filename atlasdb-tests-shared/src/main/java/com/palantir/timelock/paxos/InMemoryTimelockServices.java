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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.factory.TimeLockHelperServices;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.keyvalue.api.LockWatchCachingConfig;
import com.palantir.atlasdb.keyvalue.api.watch.LockWatchManagerInternal;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.ConjureTimelockResource;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.lock.LockService;
import com.palantir.lock.client.CommitTimestampGetter;
import com.palantir.lock.client.LegacyLeaderTimeGetter;
import com.palantir.lock.client.LegacyLockTokenUnlocker;
import com.palantir.lock.client.LockLeaseService;
import com.palantir.lock.client.NamespacedConjureTimelockServiceImpl;
import com.palantir.lock.client.RemoteTimelockServiceAdapter;
import com.palantir.lock.client.RequestBatchersFactory;
import com.palantir.lock.client.TransactionStarter;
import com.palantir.lock.v2.NamespacedTimelockRpcClient;
import com.palantir.lock.v2.TimelockService;
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
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

/* TODO(boyoruk): Delete this when we complete the JUnit5 migration. */
public final class InMemoryTimelockServices extends ExternalResource implements TimeLockServices, Closeable {
    private static final String USER_AGENT_NAME = "user-agent";
    private static final String USER_AGENT_VERSION = "3.1415926.5358979";
    private static final UserAgent USER_AGENT = UserAgent.of(UserAgent.Agent.of(USER_AGENT_NAME, USER_AGENT_VERSION));
    private static final int THREAD_POOL_SIZE = 128;
    private static final int BLOCKING_TIMEOUT_MS = 60 * 800; // 0.8 mins to ms

    private final TemporaryFolder tempFolder;

    private String client;
    private TimeLockAgent timeLockAgent;

    private TimeLockServices delegate;
    private TimeLockHelperServices helperServices;

    private LockLeaseService lockLeaseService;
    private NamespacedConjureTimelockServiceImpl namespacedConjureTimelockService;

    InMemoryTimelockServices(TemporaryFolder tempFolder) {
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

        MetricsManager metricsManager = MetricsManagers.createForTests();

        timeLockAgent = TimeLockAgent.create(
                metricsManager,
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
        createHelperServices(metricsManager);

        // Wait for leadership
        Awaitility.await()
                .atMost(Duration.ofSeconds(30L))
                .pollInterval(Duration.ofMillis(50))
                .ignoreExceptions()
                .until(() -> delegate.getTimestampService().getFreshTimestamp() > 0);
    }

    private void createHelperServices(MetricsManager metricsManager) {
        helperServices = TimeLockHelperServices.create(
                client,
                metricsManager,
                ImmutableSet.of(),
                delegate.getTimelockService(),
                LockWatchCachingConfig.builder().build(),
                Optional::empty);

        RedirectRetryTargeter redirectRetryTargeter = timeLockAgent.redirectRetryTargeter();
        ConjureTimelockService conjureTimelockService = ConjureTimelockResource.jersey(
                redirectRetryTargeter, (_namespace, _context) -> delegate.getTimelockService());
        namespacedConjureTimelockService = new NamespacedConjureTimelockServiceImpl(conjureTimelockService, client);
        lockLeaseService = LockLeaseService.create(
                namespacedConjureTimelockService,
                new LegacyLeaderTimeGetter(namespacedConjureTimelockService),
                new LegacyLockTokenUnlocker(namespacedConjureTimelockService));
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
    public AsyncTimelockService getTimelockService() {
        return delegate.getTimelockService();
    }

    @Override
    public TimestampManagementService getTimestampManagementService() {
        return delegate.getTimestampManagementService();
    }

    @Override
    public LockLog getLockLog() {
        return delegate.getLockLog();
    }

    public ManagedTimestampService getManagedTimestampService() {
        return delegate.getTimelockService();
    }

    public TimelockService getLegacyTimelockService() {
        RequestBatchersFactory requestBatchersFactory = helperServices.requestBatchersFactory();
        TransactionStarter transactionStarter = TransactionStarter.create(lockLeaseService, requestBatchersFactory);
        CommitTimestampGetter commitTimestampGetter =
                requestBatchersFactory.createBatchingCommitTimestampGetter(lockLeaseService);

        NamespacedTimelockRpcClient namespacedTimelockRpcClient =
                new InMemoryNamespacedTimelockRpcClient(getTimelockService());

        return new RemoteTimelockServiceAdapter(
                namespacedTimelockRpcClient,
                namespacedConjureTimelockService,
                lockLeaseService,
                transactionStarter,
                commitTimestampGetter);
    }

    public LockLeaseService getLockLeaseService() {
        return lockLeaseService;
    }

    public LockWatchManagerInternal getLockWatchManager() {
        return helperServices.lockWatchManager();
    }
}
