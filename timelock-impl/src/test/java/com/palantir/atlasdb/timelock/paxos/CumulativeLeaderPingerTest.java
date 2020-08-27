/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.timelock.paxos.CumulativeLeaderPinger.LastSuccessfulResult;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableLeaderPingerContext;
import com.palantir.paxos.LeaderPingResult;
import com.palantir.paxos.LeaderPingResults;
import com.palantir.paxos.LeaderPingerContext;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.Future;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CumulativeLeaderPingerTest {

    private static final Client CLIENT_WHO_IS_LED = Client.of("client-1");
    private static final Client CLIENT_WHO_IS_NOT_LED = Client.of("client-2");

    private static final HostAndPort HOST_AND_PORT = HostAndPort.fromParts("localhost", 1234);
    private static final UUID HOST_UUID = UUID.randomUUID();

    @Mock
    private BatchPingableLeader batchPingableLeader;
    private LeaderPingerContext<BatchPingableLeader> pingerWithContext;

    @Before
    public void setUp() {
        pingerWithContext = ImmutableLeaderPingerContext.of(batchPingableLeader, HOST_AND_PORT);
    }

    @Test
    public void testTooOldResultReturnsTimeOut() {
        Instant completedAt = Instant.now();
        Instant earliestCompletedDeadline = completedAt.plus(Duration.ofMillis(10));
        LastSuccessfulResult lastSuccessfulResult =
                ImmutableLastSuccessfulResult.of(completedAt, ImmutableSet.of(CLIENT_WHO_IS_LED));
        LeaderPingResult result =
                lastSuccessfulResult.result(CLIENT_WHO_IS_LED, earliestCompletedDeadline, HOST_AND_PORT, HOST_UUID);

        assertThat(result)
                .as("earliest completed deadline was not met, meaning we've timed out")
                .isEqualTo(LeaderPingResults.pingTimedOut());
    }

    @Test
    public void testClientBeingLedShowsUpAsTrue() {
        Instant completedAt = Instant.now();
        Instant earliestCompletedDeadline = completedAt.minus(Duration.ofMillis(10));
        LastSuccessfulResult lastSuccessfulResult =
                ImmutableLastSuccessfulResult.of(completedAt, ImmutableSet.of(CLIENT_WHO_IS_LED));
        LeaderPingResult result =
                lastSuccessfulResult.result(CLIENT_WHO_IS_LED, earliestCompletedDeadline, HOST_AND_PORT, HOST_UUID);

        assertThat(result)
                .isEqualTo(LeaderPingResults.pingReturnedTrue(HOST_UUID, HOST_AND_PORT));
    }

    @Test
    public void testClientNotBeingLedShowsUpAsFalse() {
        Instant completedAt = Instant.now();
        Instant earliestCompletedDeadline = completedAt.minus(Duration.ofMillis(10));
        LastSuccessfulResult lastSuccessfulResult =
                ImmutableLastSuccessfulResult.of(completedAt, ImmutableSet.of(CLIENT_WHO_IS_LED));
        LeaderPingResult result =
                lastSuccessfulResult.result(CLIENT_WHO_IS_NOT_LED, earliestCompletedDeadline, HOST_AND_PORT, HOST_UUID);

        assertThat(result)
                .isEqualTo(LeaderPingResults.pingReturnedFalse());
    }

    @Test
    public void singleIterationUpdatesInMemoryReference() {
        CumulativeLeaderPinger cumulativeLeaderPinger = new CumulativeLeaderPinger(
                pingerWithContext,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                HOST_UUID);

        Future<LeaderPingResult> clientWhoIsLed = cumulativeLeaderPinger.registerAndPing(HOST_UUID, CLIENT_WHO_IS_LED);
        Future<LeaderPingResult> clientWhoIsNotLed =
                cumulativeLeaderPinger.registerAndPing(HOST_UUID, CLIENT_WHO_IS_NOT_LED);

        when(batchPingableLeader.ping(ImmutableSet.of(CLIENT_WHO_IS_LED, CLIENT_WHO_IS_NOT_LED)))
                .thenReturn(ImmutableSet.of(CLIENT_WHO_IS_LED));

        cumulativeLeaderPinger.runOneIteration();

        assertThat(Futures.getUnchecked(clientWhoIsLed))
                .isEqualTo(LeaderPingResults.pingReturnedTrue(HOST_UUID, HOST_AND_PORT));

        assertThat(Futures.getUnchecked(clientWhoIsNotLed))
                .isEqualTo(LeaderPingResults.pingReturnedFalse());
    }

    @Test
    public void getUnresolvedFutureUntilSingleIterationHasRun() {
        CumulativeLeaderPinger cumulativeLeaderPinger = new CumulativeLeaderPinger(
                pingerWithContext,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                HOST_UUID);

        Future<LeaderPingResult> clientWhoIsLed = cumulativeLeaderPinger.registerAndPing(HOST_UUID, CLIENT_WHO_IS_LED);

        assertThat(clientWhoIsLed)
                .as("it's hard to test that it never gets resolved prior to running the single iteration")
                .isNotDone();

        when(batchPingableLeader.ping(ImmutableSet.of(CLIENT_WHO_IS_LED)))
                .thenReturn(ImmutableSet.of(CLIENT_WHO_IS_LED));

        cumulativeLeaderPinger.runOneIteration();

        assertThat(clientWhoIsLed)
                .as("we expect this to be done, everything is running within a single thread in this test")
                .isDone();
    }

    @Test
    public void getBackCachedResultIfTryingToPingTwice() {
        CumulativeLeaderPinger cumulativeLeaderPinger = new CumulativeLeaderPinger(
                pingerWithContext,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                HOST_UUID);

        Future<LeaderPingResult> clientWhoIsLed = cumulativeLeaderPinger.registerAndPing(HOST_UUID, CLIENT_WHO_IS_LED);

        when(batchPingableLeader.ping(ImmutableSet.of(CLIENT_WHO_IS_LED)))
                .thenReturn(ImmutableSet.of(CLIENT_WHO_IS_LED))
                .thenReturn(ImmutableSet.of());

        cumulativeLeaderPinger.runOneIteration();

        assertThat(Futures.getUnchecked(clientWhoIsLed))
                .isEqualTo(LeaderPingResults.pingReturnedTrue(HOST_UUID, HOST_AND_PORT));

        Future<LeaderPingResult> clientWhoIsLedSecondRequest =
                cumulativeLeaderPinger.registerAndPing(HOST_UUID, CLIENT_WHO_IS_LED);

        assertThat(Futures.getUnchecked(clientWhoIsLedSecondRequest))
                .isEqualTo(LeaderPingResults.pingReturnedTrue(HOST_UUID, HOST_AND_PORT));

        verify(batchPingableLeader, only()).ping(ImmutableSet.of(CLIENT_WHO_IS_LED));
    }

    @Test
    public void secondIterationIncludesClientsFromPreviousIterations() {
        CumulativeLeaderPinger cumulativeLeaderPinger = new CumulativeLeaderPinger(
                pingerWithContext,
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                HOST_UUID);

        cumulativeLeaderPinger.registerAndPing(HOST_UUID, CLIENT_WHO_IS_LED);

        when(batchPingableLeader.ping(ImmutableSet.of(CLIENT_WHO_IS_LED)))
                .thenReturn(ImmutableSet.of(CLIENT_WHO_IS_LED));

        cumulativeLeaderPinger.runOneIteration();
        cumulativeLeaderPinger.runOneIteration();

        // despite being registered only once, the client is included in future ping requests
        verify(batchPingableLeader, times(2)).ping(ImmutableSet.of(CLIENT_WHO_IS_LED));
    }
}
