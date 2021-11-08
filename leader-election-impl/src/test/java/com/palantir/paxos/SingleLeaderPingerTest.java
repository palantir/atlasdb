/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.leader.PingResult;
import com.palantir.leader.PingableLeader;
import com.palantir.sls.versions.OrderableSlsVersion;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SingleLeaderPingerTest {

    private static final UUID LOCAL_UUID = UUID.randomUUID();
    private static final UUID REMOTE_UUID = UUID.randomUUID();

    private static final HostAndPort HOST_AND_PORT = HostAndPort.fromParts("localhost", 8080);

    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    @Mock
    private PingableLeader pingableLeader;

    @Mock
    private GreenNodeLeadershipPrioritiser greenNodeLeadershipPrioritiser;

    @Before
    public void setup() {
        when(greenNodeLeadershipPrioritiser.shouldGreeningNodeBecomeLeader()).thenReturn(true);
    }

    @After
    public void after() {
        executorService.shutdown();
    }

    @Test
    public void recordsLeaderPingFailure() {
        RuntimeException error = new RuntimeException("foo");
        when(pingableLeader.pingV2()).thenThrow(error);
        when(pingableLeader.ping()).thenThrow(error);
        when(pingableLeader.getUUID()).thenReturn(REMOTE_UUID.toString());

        LeaderPinger pinger = pingerWithTimeout(Duration.ofSeconds(10));
        assertThat(pinger.pingLeaderWithUuid(REMOTE_UUID))
                .isEqualTo(LeaderPingResults.pingCallFailedWithExecutionException(error));
    }

    @Test
    public void recordsLeaderPingTimeout() {
        when(pingableLeader.pingV2()).thenAnswer($ -> {
            Thread.sleep(10_000);
            return PingResult.builder().isLeader(true).build();
        });

        when(pingableLeader.getUUID()).thenReturn(REMOTE_UUID.toString());

        LeaderPinger pinger = pingerWithTimeout(Duration.ofMillis(100));
        assertThat(pinger.pingLeaderWithUuid(REMOTE_UUID)).isEqualTo(LeaderPingResults.pingTimedOut());
    }

    @Test
    public void recordsLeaderPingReturnedFalse() {
        when(pingableLeader.pingV2())
                .thenReturn(PingResult.builder().isLeader(false).build());
        when(pingableLeader.getUUID()).thenReturn(REMOTE_UUID.toString());

        LeaderPinger pinger = pingerWithTimeout(Duration.ofSeconds(1));
        assertThat(pinger.pingLeaderWithUuid(REMOTE_UUID)).isEqualTo(LeaderPingResults.pingReturnedFalse());
    }

    @Test
    public void doesNotRecordLeaderPingSuccess() {
        when(pingableLeader.pingV2())
                .thenReturn(PingResult.builder().isLeader(true).build());
        when(pingableLeader.getUUID()).thenReturn(REMOTE_UUID.toString());

        LeaderPinger pinger = pingerWithTimeout(Duration.ofSeconds(1));
        assertThat(pinger.pingLeaderWithUuid(REMOTE_UUID))
                .isEqualTo(LeaderPingResults.pingReturnedTrue(REMOTE_UUID, HOST_AND_PORT));
    }

    @Test
    public void recordsLeaderPingReturnedTrueWithOlderVersion() {
        OrderableSlsVersion oldTimeLockVersion = OrderableSlsVersion.valueOf("1.1.2");
        whenRemoteLeaderHasVersion(oldTimeLockVersion);

        LeaderPinger pinger = pingerWithVersion(OrderableSlsVersion.valueOf("2.1.1"));
        assertThat(pinger.pingLeaderWithUuid(REMOTE_UUID))
                .isEqualTo(LeaderPingResults.pingReturnedTrueWithOlderVersion(oldTimeLockVersion));
    }

    @Test
    public void ignoresOlderVersionWhenGreenNodeShouldNotGainLeadership() {
        OrderableSlsVersion oldTimeLockVersion = OrderableSlsVersion.valueOf("1.1.2");
        whenRemoteLeaderHasVersion(oldTimeLockVersion);
        when(greenNodeLeadershipPrioritiser.shouldGreeningNodeBecomeLeader()).thenReturn(false);

        LeaderPinger pinger = pingerWithVersion(OrderableSlsVersion.valueOf("2.1.1"));
        assertThat(pinger.pingLeaderWithUuid(REMOTE_UUID))
                .isEqualTo(LeaderPingResults.pingReturnedTrue(REMOTE_UUID, HOST_AND_PORT));
    }

    @Test
    public void leaderPingReturnsTrueWithLeaderOnNewerVersion() {
        whenRemoteLeaderHasVersion(OrderableSlsVersion.valueOf("2.1.1"));

        LeaderPinger pinger = pingerWithVersion(OrderableSlsVersion.valueOf("1.1.1"));
        assertThat(pinger.pingLeaderWithUuid(REMOTE_UUID))
                .isEqualTo(LeaderPingResults.pingReturnedTrue(REMOTE_UUID, HOST_AND_PORT));
    }

    private void whenRemoteLeaderHasVersion(OrderableSlsVersion version) {
        when(pingableLeader.pingV2())
                .thenReturn(PingResult.builder()
                        .isLeader(true)
                        .timeLockVersion(version)
                        .build());
        when(pingableLeader.getUUID()).thenReturn(REMOTE_UUID.toString());
    }

    private LeaderPinger pingerWithTimeout(Duration leaderPingResponseWait) {
        return SingleLeaderPinger.createForTests(
                ImmutableMap.of(
                        ImmutableLeaderPingerContext.of(pingableLeader, HOST_AND_PORT),
                        new CheckedRejectionExecutorService(executorService)),
                leaderPingResponseWait,
                LOCAL_UUID,
                true,
                Optional.empty());
    }

    private LeaderPinger pingerWithVersion(OrderableSlsVersion version) {
        return new SingleLeaderPinger(
                ImmutableMap.of(
                        ImmutableLeaderPingerContext.of(pingableLeader, HOST_AND_PORT),
                        new CheckedRejectionExecutorService(executorService)),
                Duration.ofSeconds(5),
                LOCAL_UUID,
                true,
                Optional.of(version),
                greenNodeLeadershipPrioritiser);
    }
}
