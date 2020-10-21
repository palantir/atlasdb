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

package com.palantir.atlasdb.timelock.suite;

import static com.palantir.atlasdb.timelock.TemplateVariables.generateThreeNodeTimelockCluster;

import com.github.peterwippermann.junit4.parameterizedsuite.ParameterizedSuite;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.MultiLeaderMultiNodePaxosTimeLockIntegrationTest;
import com.palantir.atlasdb.timelock.MultiNodePaxosTimeLockServerIntegrationTest;
import com.palantir.atlasdb.timelock.TestableTimelockCluster;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import java.util.Collection;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;

@RunWith(ParameterizedSuite.class)
@Suite.SuiteClasses({
    MultiNodePaxosTimeLockServerIntegrationTest.class,
    MultiLeaderMultiNodePaxosTimeLockIntegrationTest.class
})
public final class MultiLeaderPaxosSuite {

    public static final TestableTimelockCluster MULTI_LEADER_PAXOS = new TestableTimelockCluster(
            "batched timestamp paxos multi leader",
            "paxosMultiServer.ftl",
            generateThreeNodeTimelockCluster(9086, builder -> builder.clientPaxosBuilder(
                            builder.clientPaxosBuilder().isUseBatchPaxosTimestamp(true))
                    .leaderMode(PaxosLeaderMode.LEADER_PER_CLIENT)));

    @Parameterized.Parameters(name = "{0}")
    public static Collection<TestableTimelockCluster> params() {
        return ImmutableSet.of(MULTI_LEADER_PAXOS);
    }

    @Rule
    @Parameterized.Parameter
    public TestableTimelockCluster cluster;
}
