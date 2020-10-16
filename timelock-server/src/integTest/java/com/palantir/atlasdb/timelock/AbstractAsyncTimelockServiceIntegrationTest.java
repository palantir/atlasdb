/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.timelock.ImmutableTemplateVariables.TimestampPaxos;
import com.palantir.timelock.config.PaxosInstallConfiguration.PaxosLeaderMode;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;

public abstract class AbstractAsyncTimelockServiceIntegrationTest {

    public static final TemplateVariables DEFAULT_SINGLE_SERVER = ImmutableTemplateVariables.builder()
            .localServerPort(9050)
            .addServerPorts()
            .clientPaxos(TimestampPaxos.builder().isUseBatchPaxosTimestamp(true).build())
            .leaderMode(PaxosLeaderMode.SINGLE_LEADER)
            .build();

    protected static final TestableTimelockCluster CLUSTER_WITH_ASYNC =
            new TestableTimelockCluster("paxosSingleServer.ftl", DEFAULT_SINGLE_SERVER);

    @ClassRule
    public static final RuleChain ASYNC_RULE_CHAIN = CLUSTER_WITH_ASYNC.getRuleChain();

    protected final TestableTimelockCluster cluster = CLUSTER_WITH_ASYNC;
}
