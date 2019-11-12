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

package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.palantir.atlasdb.timelock.suite.PaxosSuite;
import com.palantir.atlasdb.timelock.util.ExceptionMatchers;
import com.palantir.atlasdb.timelock.util.ParameterInjector;

@RunWith(Parameterized.class)
public class SingleLeaderMultiNodePaxosTimeLockIntegrationTest {

    @ClassRule
    public static ParameterInjector<TestableTimelockCluster> injector =
            ParameterInjector.withFallBackConfiguration(() -> PaxosSuite.BATCHED_TIMESTAMP_PAXOS);

    @Parameterized.Parameter
    public TestableTimelockCluster cluster;

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<TestableTimelockCluster> params() {
        return injector.getParameter();
    }

    @Test
    public void clientsCreatedDynamicallyOnNonLeadersAreFunctionalAfterFailover() {
        String client = UUID.randomUUID().toString();
        cluster.nonLeaders().forEach(server -> {
            assertThatThrownBy(() -> server.timelockServiceForClient(client).getFreshTimestamp())
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
        });

        cluster.failoverToNewLeader();

        cluster.getFreshTimestamp();
    }

    @Test
    public void clientsCreatedDynamicallyOnLeaderAreFunctionalImmediately() {
        String client = UUID.randomUUID().toString();

        cluster.currentLeader()
                .timelockServiceForClient(client)
                .getFreshTimestamp();
    }

    @Test
    public void noConflictIfLeaderAndNonLeadersSeparatelyInitializeClient() {
        String client = UUID.randomUUID().toString();
        cluster.nonLeaders().forEach(server -> {
            assertThatThrownBy(() -> server.timelockServiceForClient(client).getFreshTimestamp())
                    .satisfies(ExceptionMatchers::isRetryableExceptionWhereLeaderCannotBeFound);
        });

        long ts1 = cluster.timelockServiceForClient(client).getFreshTimestamp();

        cluster.failoverToNewLeader();

        long ts2 = cluster.getFreshTimestamp();
        assertThat(ts1).isLessThan(ts2);
    }
}
