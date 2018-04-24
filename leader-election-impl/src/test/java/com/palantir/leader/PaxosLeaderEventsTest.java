/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;

public class PaxosLeaderEventsTest {

    PaxosLeadershipEventRecorder recorder = mock(PaxosLeadershipEventRecorder.class);
    PaxosLeaderElectionService electionService = new PaxosLeaderElectionServiceBuilder()
            .proposer(mock(PaxosProposer.class))
            .knowledge(mock(PaxosLearner.class))
            .potentialLeadersToHosts(ImmutableMap.<PingableLeader, HostAndPort>of())
            .acceptors(ImmutableList.of())
            .learners(ImmutableList.of())
            .executor(Executors.newSingleThreadExecutor())
            .pingRateMs(0L)
            .randomWaitBeforeProposingLeadershipMs(0L)
            .leaderPingResponseWaitMs(0L)
            .eventRecorder(recorder)
            .onlyLogOnQuorumFailure(() -> true)
            .build();

    @Test
    public void recordsLeaderPingFailure() throws InterruptedException {
        RuntimeException error = new RuntimeException("foo");
        CompletableFuture<Boolean> pingFuture = new CompletableFuture<>();
        pingFuture.completeExceptionally(error);

        boolean result = electionService.getAndRecordLeaderPingResult(pingFuture);
        assertThat(result).isFalse();

        verify(recorder).recordLeaderPingFailure(error);
        verifyNoMoreInteractions(recorder);
    }

    @Test
    public void recordsLeaderPingTimeout() throws InterruptedException {
        // a null result from ExecutorCompletionService indicates that no results were available before the timeout
        CompletableFuture<Boolean> pingFuture = null;

        boolean result = electionService.getAndRecordLeaderPingResult(pingFuture);
        assertThat(result).isFalse();

        verify(recorder).recordLeaderPingTimeout();
        verifyNoMoreInteractions(recorder);
    }

    @Test
    public void recordsLeaderPingReturnedFalse() throws InterruptedException {
        CompletableFuture<Boolean> pingFuture = CompletableFuture.completedFuture(false);

        boolean result = electionService.getAndRecordLeaderPingResult(pingFuture);
        assertThat(result).isFalse();

        verify(recorder).recordLeaderPingReturnedFalse();
        verifyNoMoreInteractions(recorder);
    }

    @Test
    public void doesNotRecordLeaderPingSuccess() throws InterruptedException {
        CompletableFuture<Boolean> pingFuture = CompletableFuture.completedFuture(true);

        boolean result = electionService.getAndRecordLeaderPingResult(pingFuture);
        assertThat(result).isTrue();

        verifyNoMoreInteractions(recorder);
    }

}
