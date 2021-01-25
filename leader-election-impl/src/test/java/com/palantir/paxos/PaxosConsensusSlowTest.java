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
package com.palantir.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import com.palantir.common.concurrent.PTExecutors;
import java.util.concurrent.CompletionService;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PaxosConsensusSlowTest {

    Executor executor = PTExecutors.newCachedThreadPool();

    private static final int NUM_POTENTIAL_LEADERS = 6;
    private static final int QUORUM_SIZE = 4;

    private PaxosTestState state;

    @Before
    public void setup() {
        state = PaxosConsensusTestUtils.setup(NUM_POTENTIAL_LEADERS, QUORUM_SIZE);
    }

    @After
    public void teardown() throws Exception {
        PaxosConsensusTestUtils.teardown(state);
    }

    static final long NO_QUORUM_POLL_WAIT_TIME_IN_MS = 100;
    static final long QUORUM_POLL_WAIT_TIME_IN_MS = 30000;

    @SuppressWarnings("EmptyCatchBlock")
    @Test
    public void waitingOnQuorum() {
        for (int i = 0; i < NUM_POTENTIAL_LEADERS - 1; i++) {
            state.goDown(i);
        }

        CompletionService<Void> leadershipCompletionService = new ExecutorCompletionService<Void>(executor);
        leadershipCompletionService.submit(() -> {
            state.gainLeadership(NUM_POTENTIAL_LEADERS - 1);
            return null;
        });

        for (int i = 0; i < NUM_POTENTIAL_LEADERS - 1; i++) {
            try {
                if (i + 1 < QUORUM_SIZE) {
                    assertThat(leadershipCompletionService.poll(NO_QUORUM_POLL_WAIT_TIME_IN_MS, TimeUnit.MILLISECONDS))
                            .describedAs("proposer should continue to block without quorum")
                            .isNull();
                } else {
                    assertThat(leadershipCompletionService.poll(QUORUM_POLL_WAIT_TIME_IN_MS, TimeUnit.MILLISECONDS))
                            .describedAs("proposer should get leadership with quorum")
                            .isNotNull();
                    return;
                }
            } catch (InterruptedException ignored) {
            } finally {
                state.comeUp(i);
            }
        }
    }
}
