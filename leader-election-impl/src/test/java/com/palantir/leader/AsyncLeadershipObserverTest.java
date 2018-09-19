/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class AsyncLeadershipObserverTest {

    private Runnable leaderTask = mock(Runnable.class);
    private Runnable followerTask = mock(Runnable.class);
    private LeadershipObserver leadershipObserver;
    private DeterministicScheduler executorService = new DeterministicScheduler();

    @Before
    public void setUp() {
        leadershipObserver = new AsyncLeadershipObserver(executorService);
    }

    @After
    public void tearDown() {
        verifyNoMoreInteractions(leaderTask, followerTask);
    }

    @Test
    public void executeLeaderTasksAfterBecomingLeader() {
        registerFollowerAndLeaderTasks();
        gainLeadership();
        waitForTasksToFinish();

        verify(leaderTask).run();
    }

    @Test
    public void executeFollowerTasksAfterLosingLeadership() {
        registerFollowerAndLeaderTasks();
        loseLeadership();
        waitForTasksToFinish();

        verify(followerTask).run();
    }

    @Test
    public void executeAllSubmittedTasks() {
        Runnable secondLeaderTask = mock(Runnable.class);

        leadershipObserver.executeWhenGainedLeadership(leaderTask);
        leadershipObserver.executeWhenGainedLeadership(secondLeaderTask);
        gainLeadership();
        waitForTasksToFinish();

        verify(leaderTask).run();
        verify(secondLeaderTask).run();
    }

    private void registerFollowerAndLeaderTasks() {
        leadershipObserver.executeWhenGainedLeadership(leaderTask);
        leadershipObserver.executeWhenLostLeadership(followerTask);
    }

    private void gainLeadership() {
        leadershipObserver.gainedLeadership();
    }

    private void loseLeadership() {
        leadershipObserver.lostLeadership();
    }

    private void waitForTasksToFinish() {
        executorService.runUntilIdle();
    }
}
