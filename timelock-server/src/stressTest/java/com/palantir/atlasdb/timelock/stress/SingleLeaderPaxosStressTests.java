/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.stress;

import static com.palantir.atlasdb.timelock.TemplateVariables.generateThreeNodeTimelockCluster;

import com.github.peterwippermann.junit4.parameterizedsuite.ParameterizedSuite;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.MultiNodePaxosTimeLockServerStressTest;
import com.palantir.atlasdb.timelock.TestableTimelockCluster;
import com.palantir.atlasdb.timelock.util.TestableTimeLockClusterPorts;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import java.util.Collection;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(ParameterizedSuite.class)
@Suite.SuiteClasses(MultiNodePaxosTimeLockServerStressTest.class)
public final class SingleLeaderPaxosStressTests {

    public static final TestableTimelockCluster NON_BATCHED_TIMESTAMP_PAXOS = new TestableTimelockCluster(
            "non-batched timestamp paxos single leader",
            "paxosMultiServer.ftl",
            generateThreeNodeTimelockCluster(
                    TestableTimeLockClusterPorts.SINGLE_LEADER_PAXOS_STRESS_TESTS_1,
                    builder -> builder.clientPaxosBuilder(
                                    builder.clientPaxosBuilder().isUseBatchPaxosTimestamp(false))
                            .leaderMode(PaxosLeaderMode.SINGLE_LEADER)));

    public static final TestableTimelockCluster BATCHED_TIMESTAMP_PAXOS = new TestableTimelockCluster(
            "batched timestamp paxos single leader",
            "paxosMultiServer.ftl",
            generateThreeNodeTimelockCluster(
                    TestableTimeLockClusterPorts.SINGLE_LEADER_PAXOS_STRESS_TESTS_2,
                    builder -> builder.clientPaxosBuilder(
                                    builder.clientPaxosBuilder().isUseBatchPaxosTimestamp(true))
                            .leaderMode(PaxosLeaderMode.SINGLE_LEADER)));

    public static final TestableTimelockCluster BATCHED_PAXOS = new TestableTimelockCluster(
            "batched single leader",
            "paxosMultiServer.ftl",
            generateThreeNodeTimelockCluster(
                    TestableTimeLockClusterPorts.SINGLE_LEADER_PAXOS_STRESS_TESTS_3,
                    builder -> builder.clientPaxosBuilder(builder.clientPaxosBuilder()
                                    .isUseBatchPaxosTimestamp(false)
                                    .isBatchSingleLeader(true))
                            .leaderMode(PaxosLeaderMode.SINGLE_LEADER)));

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestableTimelockCluster> params() {
        return ImmutableSet.of(NON_BATCHED_TIMESTAMP_PAXOS, BATCHED_TIMESTAMP_PAXOS, BATCHED_PAXOS);
    }

    @Rule
    @Parameterized.Parameter
    public TestableTimelockCluster cluster;
}
