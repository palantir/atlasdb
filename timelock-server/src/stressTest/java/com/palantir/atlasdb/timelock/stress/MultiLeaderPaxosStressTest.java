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

package com.palantir.atlasdb.timelock.stress;

import static com.palantir.atlasdb.timelock.TemplateVariables.generateThreeNodeTimelockCluster;

import com.palantir.atlasdb.timelock.AbstractStressTest;
import com.palantir.atlasdb.timelock.TestableTimelockClusterV2;
import com.palantir.atlasdb.timelock.util.TestableTimeLockClusterPorts;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import org.junit.jupiter.api.extension.RegisterExtension;

public class MultiLeaderPaxosStressTest extends AbstractStressTest {
    @RegisterExtension
    public static final TestableTimelockClusterV2 MULTI_LEADER_PAXOS = new TestableTimelockClusterV2(
            "batched timestamp paxos multi leader",
            "paxosMultiServer.ftl",
            generateThreeNodeTimelockCluster(
                    TestableTimeLockClusterPorts.MULTI_LEADER_PAXOS_STRESS_TESTS, builder -> builder.clientPaxosBuilder(
                                    builder.clientPaxosBuilder().isUseBatchPaxosTimestamp(true))
                            .leaderMode(PaxosLeaderMode.LEADER_PER_CLIENT)));

    public MultiLeaderPaxosStressTest() {
        super(MULTI_LEADER_PAXOS);
    }
}
