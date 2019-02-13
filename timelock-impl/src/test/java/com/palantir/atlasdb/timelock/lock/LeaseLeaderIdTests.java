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

package com.palantir.atlasdb.timelock.lock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.codahale.metrics.MetricRegistry;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.AsyncTimelockServiceImpl;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.lock.v2.LeaderTime;

@RunWith(MockitoJUnitRunner.class)
public class LeaseLeaderIdTests {
    @Mock private LeaderElectionService leaderElectionService;
    @Mock private LeaderElectionService.LeadershipToken leadershipToken;

    private final LockLog lockLog = new LockLog(new MetricRegistry(), () -> 2L);
    private AsyncTimelockService timelockService;

    @Before
    public void before() {
        when(leaderElectionService.getCurrentTokenIfLeading()).thenReturn(Optional.of(leadershipToken));
        when(leaderElectionService.getSuspectedLeaderInMemory()).thenReturn(Optional.empty());
        when(leaderElectionService.isStillLeading(leadershipToken)).thenReturn(
                LeaderElectionService.StillLeadingStatus.LEADING);

        timelockService = AwaitingLeadershipProxy.newProxyInstance(
                AsyncTimelockService.class,
                this::createAsynTimelockService,
                leaderElectionService);
    }

    @Test
    public void shouldServeSameLeaderIdForSuccessiveCalls() {
        LeaderTime leaderTime1 = timelockService.leaderTime();
        LeaderTime leaderTime2 = timelockService.leaderTime();

        assertThat(leaderTime2.isComparableWith(leaderTime1)).isTrue();
    }

    @Test
    public void shouldServeDifferentLeaderIdAfterLeaderElection() {
        LeaderTime leaderTime1 = timelockService.leaderTime();

        when(leaderElectionService.isStillLeading(leadershipToken)).thenReturn(
                LeaderElectionService.StillLeadingStatus.NOT_LEADING);

        assertThatThrownBy(() -> timelockService.leaderTime()).isInstanceOf(NotCurrentLeaderException.class);

        when(leaderElectionService.isStillLeading(leadershipToken)).thenReturn(
                LeaderElectionService.StillLeadingStatus.LEADING);

        LeaderTime leaderTime2 = timelockService.leaderTime();
        assertThat(leaderTime2.isComparableWith(leaderTime1)).isFalse();
    }

    private AsyncTimelockServiceImpl createAsynTimelockService() {
        ScheduledExecutorService reaperExecutor = PTExecutors.newSingleThreadScheduledExecutor();
        ScheduledExecutorService timeoutExecutor = PTExecutors.newSingleThreadScheduledExecutor();
        return new AsyncTimelockServiceImpl(
                AsyncLockService.createDefault(lockLog, reaperExecutor, timeoutExecutor),
                mock(ManagedTimestampService.class));
    }

}
