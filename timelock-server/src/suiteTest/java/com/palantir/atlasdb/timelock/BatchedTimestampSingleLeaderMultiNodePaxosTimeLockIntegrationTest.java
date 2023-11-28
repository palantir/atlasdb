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
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import org.junit.jupiter.api.extension.RegisterExtension;

public class BatchedTimestampSingleLeaderMultiNodePaxosTimeLockIntegrationTest
        extends AbstractSingleLeaderMultiNodePaxosTimeLockIntegrationTest {

    @RegisterExtension
    public static final TestableTimelockClusterV2 BATCHED_TIMESTAMP_PAXOS = new TestableTimelockClusterV2(
            "batched timestamp paxos single leader",
            "paxosMultiServer.ftl",
            generateThreeNodeTimelockCluster(
                    TestableTimeLockClusterPorts.SINGLE_LEADER_PAXOS_SUITE_2, builder -> builder.clientPaxosBuilder(
                                    builder.clientPaxosBuilder().isUseBatchPaxosTimestamp(true))
                            .leaderMode(PaxosLeaderMode.SINGLE_LEADER)));

    public BatchedTimestampSingleLeaderMultiNodePaxosTimeLockIntegrationTest() {
        super(BATCHED_TIMESTAMP_PAXOS);
    }
}
