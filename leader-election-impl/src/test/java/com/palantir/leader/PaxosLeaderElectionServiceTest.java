/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.Executors;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;

public class PaxosLeaderElectionServiceTest {
    @Test
    public void weAreOneOfThePotentialLeaders() throws Exception {
        PingableLeader other = mock(PingableLeader.class);
        PaxosLeaderElectionService service = new PaxosLeaderElectionServiceBuilder()
                .proposer(mock(PaxosProposer.class))
                .knowledge(mock(PaxosLearner.class))
                .potentialLeadersToHosts(ImmutableMap.of(other, HostAndPort.fromHost("other")))
                .acceptors(ImmutableList.of())
                .learners(ImmutableList.of())
                .executor(Executors.newSingleThreadExecutor())
                .pingRateMs(0L)
                .randomWaitBeforeProposingLeadershipMs(0L)
                .leaderPingResponseWaitMs(0L)
                .eventRecorder(mock(PaxosLeadershipEventRecorder.class))
                .build();

        assertThat(service.getPotentialLeaders()).containsExactlyInAnyOrder(other, service);
    }

}
