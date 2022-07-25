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

package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureLockTokenV2;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.util.TimelockTestUtils;
import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.impl.TooManyRequestsException;
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LockToken;
import com.palantir.timestamp.TimestampRange;
import com.palantir.tokens.auth.AuthHeader;
import java.net.URL;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConjureTimelockResourceTest {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer test");
    private static final int REMOTE_PORT = 4321;
    private static final URL LOCAL = TimelockTestUtils.url("https://localhost:1234");
    private static final URL REMOTE = TimelockTestUtils.url("https://localhost:" + REMOTE_PORT);
    private static final RedirectRetryTargeter TARGETER =
            RedirectRetryTargeter.create(LOCAL, ImmutableList.of(LOCAL, REMOTE));

    private static final String NAMESPACE = "test";

    @Mock
    private AsyncTimelockService timelockService;

    @Mock
    private LeaderTime leaderTime;

    private ConjureTimelockResource resource;
    private ConjureTimelockService service;

    @Before
    public void before() {
        resource = new ConjureTimelockResource(TARGETER, unused -> timelockService);
        service = ConjureTimelockResource.jersey(TARGETER, unused -> timelockService);
        when(timelockService.leaderTime()).thenReturn(Futures.immediateFuture(leaderTime));
    }

    @Test
    public void canGetLeaderTime() {
        assertThat(Futures.getUnchecked(resource.leaderTime(AUTH_HEADER, NAMESPACE)))
                .isEqualTo(leaderTime);
    }

    @Test
    public void canGetTimestampsUsingSingularAndBatchedMethods() {
        TimestampRange firstRange = TimestampRange.createInclusiveRange(1L, 2L);
        TimestampRange secondRange = TimestampRange.createInclusiveRange(3L, 4L);
        TimestampRange thirdRange = TimestampRange.createInclusiveRange(5L, 5L);

        when(timelockService.getFreshTimestampsAsync(anyInt()))
                .thenReturn(Futures.immediateFuture(firstRange))
                .thenReturn(Futures.immediateFuture(secondRange))
                .thenReturn(Futures.immediateFuture(thirdRange));

        assertThat(Futures.getUnchecked(
                        resource.getFreshTimestamps(AUTH_HEADER, NAMESPACE, ConjureGetFreshTimestampsRequest.of(2))))
                .satisfies(response -> {
                    assertThat(response.getInclusiveLower()).isEqualTo(firstRange.getLowerBound());
                    assertThat(response.getInclusiveUpper()).isEqualTo(firstRange.getUpperBound());
                });
        assertThat(Futures.getUnchecked(resource.getFreshTimestampsV2(
                                AUTH_HEADER, NAMESPACE, ConjureGetFreshTimestampsRequestV2.of(2)))
                        .get())
                .satisfies(range -> {
                    assertThat(range.getStart()).isEqualTo(secondRange.getLowerBound());
                    assertThat(range.getCount()).isEqualTo(secondRange.size());
                });
        assertThat(Futures.getUnchecked(resource.getFreshTimestamp(AUTH_HEADER, NAMESPACE))
                        .get())
                .isEqualTo(thirdRange.getLowerBound());
    }

    @Test
    public void canUnlockUsingV1AndV2Endpoints() {
        UUID tokenOne = UUID.randomUUID();
        UUID tokenTwo = UUID.randomUUID();
        UUID tokenThree = UUID.randomUUID();

        Set<ConjureLockToken> requestOne = ImmutableSet.of(ConjureLockToken.of(tokenOne));

        Set<LockToken> setOne = ImmutableSet.of(LockToken.of(tokenTwo));
        Set<LockToken> setTwo = ImmutableSet.of(LockToken.of(tokenThree));

        when(timelockService.unlock(any()))
                .thenReturn(Futures.immediateFuture(setOne))
                .thenReturn(Futures.immediateFuture(setTwo));

        ConjureUnlockResponse unlockResponse =
                Futures.getUnchecked(resource.unlock(AUTH_HEADER, NAMESPACE, ConjureUnlockRequest.of(requestOne)));
        assertThat(unlockResponse.getTokens()).containsExactly(ConjureLockToken.of(tokenTwo));
        verify(timelockService).unlock(eq(ImmutableSet.of(LockToken.of(tokenOne))));

        ConjureUnlockResponseV2 secondResponse = Futures.getUnchecked(resource.unlockV2(
                AUTH_HEADER, NAMESPACE, ConjureUnlockRequestV2.of(ImmutableSet.of(ConjureLockTokenV2.of(tokenThree)))));

        assertThat(secondResponse.getTokens()).containsExactly(ConjureLockTokenV2.of(tokenThree));
        verify(timelockService).unlock(eq(ImmutableSet.of(LockToken.of(tokenThree))));
    }

    @Test
    public void jerseyPropagatesExceptions() {
        when(resource.leaderTime(AUTH_HEADER, NAMESPACE)).thenThrow(new BlockingTimeoutException(""));
        assertQosExceptionThrownBy(
                Futures.submitAsync(
                        () -> Futures.immediateFuture(service.leaderTime(AUTH_HEADER, NAMESPACE)),
                        MoreExecutors.directExecutor()),
                new AssertVisitor() {
                    @Override
                    public Void visit(QosException.Throttle exception) {
                        assertThat(exception.getRetryAfter()).contains(Duration.ZERO);
                        return null;
                    }
                });
    }

    @Test
    public void handlesBlockingTimeout() {
        when(resource.leaderTime(AUTH_HEADER, NAMESPACE)).thenThrow(new BlockingTimeoutException(""));
        assertQosExceptionThrownBy(resource.leaderTime(AUTH_HEADER, NAMESPACE), new AssertVisitor() {
            @Override
            public Void visit(QosException.Throttle exception) {
                assertThat(exception.getRetryAfter()).contains(Duration.ZERO);
                return null;
            }
        });
    }

    @Test
    public void handlesTooManyRequestsException() {
        when(resource.leaderTime(AUTH_HEADER, NAMESPACE)).thenThrow(new TooManyRequestsException(""));
        assertQosExceptionThrownBy(resource.leaderTime(AUTH_HEADER, NAMESPACE), new AssertVisitor() {
            @Override
            public Void visit(QosException.Throttle exception) {
                assertThat(exception.getRetryAfter()).isEmpty();
                return null;
            }
        });
    }

    @Test
    public void handlesNotCurrentLeader() {
        when(resource.leaderTime(AUTH_HEADER, NAMESPACE))
                .thenThrow(new NotCurrentLeaderException("", HostAndPort.fromParts("localhost", REMOTE_PORT)));
        assertQosExceptionThrownBy(resource.leaderTime(AUTH_HEADER, NAMESPACE), new AssertVisitor() {
            @Override
            public Void visit(QosException.RetryOther exception) {
                assertThat(exception.getRedirectTo()).isEqualTo(REMOTE);
                return null;
            }
        });
    }

    private static void assertQosExceptionThrownBy(ListenableFuture<?> future, AssertVisitor visitor) {
        try {
            Futures.getDone(future);
            fail("Future was expected to error");
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            assertThat(cause).isInstanceOf(QosException.class);
            ((QosException) cause).accept(visitor);
        }
    }

    private abstract static class AssertVisitor implements QosException.Visitor<Void> {

        @Override
        public Void visit(QosException.Throttle exception) {
            fail("Did not expect throttle");
            return null;
        }

        @Override
        public Void visit(QosException.RetryOther exception) {
            fail("Did not expect retry other");
            return null;
        }

        @Override
        public Void visit(QosException.Unavailable exception) {
            fail("Did not expect unavailable");
            return null;
        }
    }
}
