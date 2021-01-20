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
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.impl.TooManyRequestsException;
import com.palantir.lock.remoting.BlockingTimeoutException;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
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
    private static final URL LOCAL = url("https://localhost:1234");
    private static final URL REMOTE = url("https://localhost:" + REMOTE_PORT);
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
        resource = new ConjureTimelockResource(TARGETER, unused -> timelockService, 4);
        service = ConjureTimelockResource.jersey(TARGETER, unused -> timelockService, 4);
        when(timelockService.leaderTime()).thenReturn(Futures.immediateFuture(leaderTime));
    }

    @Test
    public void canGetLeaderTime() {
        assertThat(Futures.getUnchecked(resource.leaderTime(AUTH_HEADER, NAMESPACE)))
                .isEqualTo(leaderTime);
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

    private static URL url(String url) {
        try {
            return new URL(url);
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
