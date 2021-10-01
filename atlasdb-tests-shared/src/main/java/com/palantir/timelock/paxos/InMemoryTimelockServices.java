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
import com.palantir.atlasdb.timelock.AsyncTimelockResource;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.lock.LockService;
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
import com.palantir.timestamp.TimestampService;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
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

    private static File tryCreateSubFolder(TemporaryFolder tempFolder) {
        try {
            return tempFolder.newFolder();
        } catch (IOException e) {
            throw new SafeRuntimeException("Failed to create temporary folder", e);
        }
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

        TimeLockServices services = timeLockAgent.createInvalidatingTimeLockServices("client");

        // Wait for leadership
        Awaitility.await()
                .atMost(Duration.ofSeconds(30L))
                .pollInterval(Duration.ofMillis(50))
                .ignoreExceptions()
                .until(() -> services.getTimestampService().getFreshTimestamp() > 0);

        return new InMemoryTimelockServices(services, timeLockAgent);
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
}
