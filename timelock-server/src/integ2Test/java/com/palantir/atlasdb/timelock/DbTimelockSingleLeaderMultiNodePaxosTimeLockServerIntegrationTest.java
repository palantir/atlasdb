/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import static com.palantir.atlasdb.timelock.TemplateVariables.generateThreeNodeTimelockCluster;

import com.palantir.atlasdb.timelock.util.TestableTimeLockClusterPorts;
import com.palantir.timelock.config.PaxosInstallConfiguration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.junit.jupiter.api.extension.RegisterExtension;

public class DbTimelockSingleLeaderMultiNodePaxosTimeLockServerIntegrationTest
        extends AbstractMultiNodePaxosTimeLockServerIntegrationTest {
    private static final Iterable<TemplateVariables> TEMPLATE_VARIABLES = generateThreeNodeTimelockCluster(
            TestableTimeLockClusterPorts.DB_TIMELOCK_SINGLE_LEADER_PAXOS_SUITE,
            builder -> builder.clientPaxosBuilder(builder.clientPaxosBuilder()
                            .isUseBatchPaxosTimestamp(false)
                            .isBatchSingleLeader(true))
                    .leaderMode(PaxosInstallConfiguration.PaxosLeaderMode.SINGLE_LEADER));
    private static final List<TestableTimelockServerConfiguration> TESTABLE_CONFIGURATIONS = StreamSupport.stream(
                    TEMPLATE_VARIABLES.spliterator(), false)
            .map(variables -> TestableTimelockServerConfiguration.of(variables, true))
            .collect(Collectors.toList());

    @RegisterExtension
    public static final TestableTimelockClusterV2 DB_TIMELOCK_CLUSTER = new TestableTimelockClusterV2(
            "db-timelock; batched single leader", "dbTimeLockPaxosMultiServer.ftl", TESTABLE_CONFIGURATIONS);

    public DbTimelockSingleLeaderMultiNodePaxosTimeLockServerIntegrationTest() {
        super(DB_TIMELOCK_CLUSTER);
    }
}
