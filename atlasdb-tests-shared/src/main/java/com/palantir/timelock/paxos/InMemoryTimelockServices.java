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
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.timelock.AsyncTimelockResource;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.LockMode;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockService;
import com.palantir.lock.SimpleTimeDuration;
import com.palantir.lock.impl.LockTokenConverter;
import com.palantir.lock.v2.ClientLockingOptions;
import com.palantir.lock.v2.IdentifiedTimeLockRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.RefreshLockResponseV2;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.v2.TimelockService;
import com.palantir.lock.v2.TimestampAndPartition;
import com.palantir.lock.v2.WaitForLocksRequest;
import com.palantir.lock.v2.WaitForLocksResponse;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.refreshable.Refreshable;
import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.timelock.config.ClusterInstallConfiguration;
import com.palantir.timelock.config.ImmutableClusterInstallConfiguration;
import com.palantir.timelock.config.ImmutableDefaultClusterConfiguration;
import com.palantir.timelock.config.ImmutableTimeLockRuntimeConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampRange;
import com.palantir.timestamp.TimestampService;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.awaitility.Awaitility;
import org.junit.rules.TemporaryFolder;

public final class InMemoryTimelockServices implements TimeLockServices, Closeable {
    private static final String USER_AGENT_NAME = "user-agent";
    private static final String USER_AGENT_VERSION = "3.1415926.5358979";
    private static final UserAgent USER_AGENT = UserAgent.of(UserAgent.Agent.of(USER_AGENT_NAME, USER_AGENT_VERSION));
    private static final int THREAD_POOL_SIZE = 128;
    private static final int BLOCKING_TIMEOUT_MS = 60 * 800; // 0.8 mins to ms

    private final TimeLockServices delegate;
    private final TimeLockAgent timeLockAgent;

    private InMemoryTimelockServices(TimeLockServices delegate, TimeLockAgent timeLockAgent) {
        this.delegate = delegate;
        this.timeLockAgent = timeLockAgent;
    }

    @Override
    public void close() {
        timeLockAgent.shutdown();
    }

    public static InMemoryTimelockServices create(TemporaryFolder tempFolder) {
        return create(tryCreateSubFolder(tempFolder));
    }

    private static InMemoryTimelockServices create(File dataDirectory) {
        PaxosInstallConfiguration paxos = PaxosInstallConfiguration.builder()
                .dataDirectory(dataDirectory)
                .leaderMode(PaxosLeaderMode.SINGLE_LEADER)
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

        TimeLockAgent timeLockAgent = TimeLockAgent.create(
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

        // TODO(gs): Currently hard-coded to one client.
        //  We might want to refactor this to allow us to write a test that uses multiple clients
        TimeLockServices services = timeLockAgent.createInvalidatingTimeLockServices("client");

        // Wait for leadership
        Awaitility.await()
                .atMost(Duration.ofSeconds(30L))
                .pollInterval(Duration.ofMillis(50))
                .ignoreExceptions()
                .until(() -> services.getTimestampService().getFreshTimestamp() > 0);

        return new InMemoryTimelockServices(services, timeLockAgent);
    }

    private static File tryCreateSubFolder(TemporaryFolder tempFolder) {
        try {
            return tempFolder.newFolder();
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

    public TimelockService getLegacyTimelockService() {
        return new DelegatingTimelockService();
    }

    private class DelegatingTimelockService implements TimelockService {
        private final AsyncTimelockService timelock = getTimelockService();

        @Override
        public long getFreshTimestamp() {
            return timelock.getFreshTimestamp();
        }

        @Override
        public long getCommitTimestamp(long _startTs, LockToken _commitLocksToken) {
            return getFreshTimestamp();
        }

        @Override
        public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
            return timelock.getFreshTimestamps(numTimestampsRequested);
        }

        @Override
        public LockImmutableTimestampResponse lockImmutableTimestamp() {
            return timelock.lockImmutableTimestamp(IdentifiedTimeLockRequest.create());
        }

        // TODO(gs): copied from LegacyTimelockService
        @Override
        public List<StartIdentifiedAtlasDbTransactionResponse> startIdentifiedAtlasDbTransactionBatch(int count) {
            // Track these separately in the case that getFreshTimestamp fails but lockImmutableTimestamp succeeds
            List<LockImmutableTimestampResponse> immutableTimestampLocks = new ArrayList<>();
            List<StartIdentifiedAtlasDbTransactionResponse> responses = new ArrayList<>();
            try {
                IntStream.range(0, count).forEach($ -> {
                    LockImmutableTimestampResponse immutableTimestamp = lockImmutableTimestamp();
                    immutableTimestampLocks.add(immutableTimestamp);
                    responses.add(StartIdentifiedAtlasDbTransactionResponse.of(
                            immutableTimestamp, TimestampAndPartition.of(getFreshTimestamp(), 0)));
                });
                return responses;
            } catch (RuntimeException | Error throwable) {
                try {
                    unlock(immutableTimestampLocks.stream()
                            .map(LockImmutableTimestampResponse::getLock)
                            .collect(Collectors.toSet()));
                } catch (Throwable unlockThrowable) {
                    throwable.addSuppressed(unlockThrowable);
                }
                throw throwable;
            }
        }

        @Override
        public long getImmutableTimestamp() {
            return timelock.getImmutableTimestamp();
        }

        @Override
        public LockResponse lock(LockRequest request) {
            LockRefreshToken legacyToken = lockAnonymous(toLegacyLockRequest(request));
            if (legacyToken == null) {
                return LockResponse.timedOut();
            } else {
                return LockResponse.successful(LockTokenConverter.toTokenV2(legacyToken));
            }
        }

        @Override
        public LockResponse lock(LockRequest lockRequest, ClientLockingOptions options) {
            return lock(lockRequest);
        }

        @Override
        public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
            return tryGet(timelock.waitForLocks(request));
        }

        @Override
        public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
            ListenableFuture<RefreshLockResponseV2> future = timelock.refreshLockLeases(tokens);
            return tryGet(future).refreshedTokens();
        }

        @Override
        public Set<LockToken> unlock(Set<LockToken> tokens) {
            return tryGet(timelock.unlock(tokens));
        }

        @Override
        public void tryUnlock(Set<LockToken> tokens) {
            // TODO(gs): swallow exceptions?
            unlock(tokens);
        }

        @Override
        public long currentTimeMillis() {
            return timelock.currentTimeMillis();
        }

        private <T> T tryGet(ListenableFuture<T> future) {
            try {
                return future.get(5L, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new SafeRuntimeException("Async request failed", e);
            }
        }

        // TODO(gs): utilities copied from LegacyTimelockService
        private LockRefreshToken lockAnonymous(com.palantir.lock.LockRequest request) {
            try {
                // TODO(gs): use fixed client
                return getLockService().lock(LockClient.ANONYMOUS.getClientId(), request);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }

        private com.palantir.lock.LockRequest toLegacyLockRequest(LockRequest request) {
            SortedMap<LockDescriptor, LockMode> locks = buildLockMap(request.getLockDescriptors(), LockMode.WRITE);
            return com.palantir.lock.LockRequest.builder(locks)
                    .blockForAtMost(SimpleTimeDuration.of(request.getAcquireTimeoutMs(), TimeUnit.MILLISECONDS))
                    .build();
        }

        private SortedMap<LockDescriptor, LockMode> buildLockMap(
                Set<LockDescriptor> lockDescriptors, LockMode lockMode) {
            SortedMap<LockDescriptor, LockMode> locks = new TreeMap<>();
            for (LockDescriptor descriptor : lockDescriptors) {
                locks.put(descriptor, lockMode);
            }
            return locks;
        }
    }
}
