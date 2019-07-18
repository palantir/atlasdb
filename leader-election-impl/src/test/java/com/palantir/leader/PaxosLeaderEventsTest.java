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
package com.palantir.leader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;
import com.palantir.paxos.LeaderPinger;
import com.palantir.paxos.SingleLeaderPinger;

@RunWith(MockitoJUnitRunner.class)
public class PaxosLeaderEventsTest {

    private static final UUID LOCAL_UUID = UUID.randomUUID();
    private static final UUID REMOTE_UUID = UUID.randomUUID();

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @Mock private PingableLeader pingableLeader;
    @Mock private PaxosLeadershipEventRecorder recorder;

    @After
    public void after() {
        executorService.shutdown();
    }

    @Test
    public void recordsLeaderPingFailure() {
        RuntimeException error = new RuntimeException("foo");
        when(pingableLeader.ping()).thenThrow(error);
        when(pingableLeader.getUUID()).thenReturn(REMOTE_UUID.toString());

        LeaderPinger pinger = pingerWithTimeout(Duration.ofSeconds(10));
        assertThat(pinger.pingLeaderWithUuid(REMOTE_UUID)).isFalse();

        verify(recorder).recordLeaderPingFailure(error);
        verifyNoMoreInteractions(recorder);
    }

    @Test
    public void recordsLeaderPingTimeout() {
        when(pingableLeader.ping()).thenAnswer(bla -> {
            Thread.sleep(10_000);
            return true;
        });

        when(pingableLeader.getUUID()).thenReturn(REMOTE_UUID.toString());

        LeaderPinger pinger = pingerWithTimeout(Duration.ofMillis(100));
        assertThat(pinger.pingLeaderWithUuid(REMOTE_UUID)).isFalse();

        verify(recorder).recordLeaderPingTimeout();
        verifyNoMoreInteractions(recorder);
    }

    @Test
    public void recordsLeaderPingReturnedFalse() {
        when(pingableLeader.ping()).thenReturn(false);
        when(pingableLeader.getUUID()).thenReturn(REMOTE_UUID.toString());

        LeaderPinger pinger = pingerWithTimeout(Duration.ofSeconds(1));
        assertThat(pinger.pingLeaderWithUuid(REMOTE_UUID)).isFalse();

        verify(recorder).recordLeaderPingReturnedFalse();
        verifyNoMoreInteractions(recorder);
    }

    @Test
    public void doesNotRecordLeaderPingSuccess() {
        when(pingableLeader.ping()).thenReturn(true);
        when(pingableLeader.getUUID()).thenReturn(REMOTE_UUID.toString());

        LeaderPinger pinger = pingerWithTimeout(Duration.ofSeconds(1));
        assertThat(pinger.pingLeaderWithUuid(REMOTE_UUID)).isTrue();

        verifyZeroInteractions(recorder);
    }

    private LeaderPinger pingerWithTimeout(Duration leaderPingResponseWait) {
        return new SingleLeaderPinger(
                ImmutableMap.of(pingableLeader, executorService),
                leaderPingResponseWait,
                recorder,
                LOCAL_UUID);
    }

}
