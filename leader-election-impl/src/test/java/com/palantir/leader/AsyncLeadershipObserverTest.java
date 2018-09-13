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

import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class AsyncLeadershipObserverTest {

    private Runnable leaderTask = mock(Runnable.class);
    private Runnable followerTask = mock(Runnable.class);
    private LeadershipObserver leadershipObserver;


    @Before
    public void setUp() {
        leadershipObserver = new AsyncLeadershipObserver();
        leadershipObserver.executeWhenGainedLeadership(leaderTask);
        leadershipObserver.executeWhenLostLeadership(followerTask);
    }

    @Test
    public void executeLeaderTasksAfterBecomingLeader() {
        gainLeadership();
        verify(leaderTask, times(1)).run();
        verify(followerTask, times(0)).run();
    }

    @Test
    public void executeFollowerTasksAfterLosingLeadership() {
        loseLeadership();
        verify(followerTask, times(1)).run();
        verify(leaderTask, times(0)).run();
    }

    private void gainLeadership() {
        leadershipObserver.gainedLeadership();
    }

    private void loseLeadership() {
        leadershipObserver.lostLeadership();
    }
}