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

package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.service.PartialServiceConfiguration;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.refreshable.Refreshable;
import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.timelock.config.ClusterInstallConfiguration;
import com.palantir.timelock.config.ImmutableClusterInstallConfiguration;
import com.palantir.timelock.config.ImmutableDefaultClusterConfiguration;
import com.palantir.timelock.config.ImmutableTimeLockRuntimeConfiguration;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.timelock.paxos.TimeLockAgent;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TimeLockInMemoryTest {
    private static final String USER_AGENT_NAME = "user-agent";
    private static final String USER_AGENT_VERSION = "3.1415926.5358979";
    private static final UserAgent USER_AGENT = UserAgent.of(UserAgent.Agent.of(USER_AGENT_NAME, USER_AGENT_VERSION));
    private static final int THREAD_POOL_SIZE = 128;
    private static final int BLOCKING_TIMEOUT_MS = 60 * 800; // 0.8 mins to ms

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private TimeLockInstallConfiguration install;
    private TimeLockRuntimeConfiguration runtime;

    private TimeLockAgent timeLockAgent;

    @Before
    public void setup() throws IOException {
        PaxosInstallConfiguration paxos = PaxosInstallConfiguration.builder()
                .dataDirectory(tempFolder.getRoot())
                .isNewService(false)
                .build();

        ClusterInstallConfiguration cluster = ImmutableClusterInstallConfiguration.builder()
                .enableNonstandardAndPossiblyDangerousTopology(true)
                .build();
        install = TimeLockInstallConfiguration.builder()
                .paxos(paxos)
                .cluster(cluster)
                .build();

        ImmutableDefaultClusterConfiguration clusterConfig = ImmutableDefaultClusterConfiguration.builder()
                .localServer("local")
                .cluster(PartialServiceConfiguration.of(List.of("local"), Optional.empty()))
                .build();
        runtime = ImmutableTimeLockRuntimeConfiguration.builder()
                .clusterSnapshot(clusterConfig)
                .build();

        timeLockAgent = TimeLockAgent.create(
                MetricsManagers.of(new MetricRegistry(), SharedTaggedMetricRegistries.getSingleton()),
                install,
                Refreshable.only(runtime), // This won't actually live reload.
                runtime.clusterSnapshot(),
                USER_AGENT,
                THREAD_POOL_SIZE,
                BLOCKING_TIMEOUT_MS,
                _unused -> {}, // TODO(gs): what is registrar used for?
                Optional.empty(),
                OrderableSlsVersion.valueOf("0.0.0"),
                ObjectMappers.newServerObjectMapper(),
                () -> System.exit(0));
    }

    @Test
    public void comesUpHealthy() {
        assertThat(timeLockAgent.getStatus())
                .hasValueSatisfying(
                        digest -> assertThat(digest.statusesToClient().size()).isGreaterThan(0));
    }
}
