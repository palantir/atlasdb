/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.leader.proxy;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.leader.PaxosLeadershipToken;

public class AwaitingLeadershipProxyTest {
    private static final String TEST_MESSAGE = "test_message";

    @Test
    @SuppressWarnings("SelfEquals")
    // We're asserting that calling .equals on a proxy does not redirect the .equals call to the instance its being proxied.
    public void shouldAllowObjectMethodsWhenLeading() throws Exception {
        Runnable mockRunnable = mock(Runnable.class);
        Supplier<Runnable> delegateSupplier = Suppliers.ofInstance(mockRunnable);
        LeaderElectionService mockLeader = mock(LeaderElectionService.class);

        when(mockLeader.getSuspectedLeaderInMemory()).thenReturn(Optional.empty());
        when(mockLeader.isStillLeading(any(LeaderElectionService.LeadershipToken.class)))
                .thenReturn(LeaderElectionService.StillLeadingStatus.LEADING);

        Runnable proxy = AwaitingLeadershipProxy.newProxyInstance(Runnable.class, delegateSupplier, mockLeader);

        assertThat(proxy.hashCode()).isNotNull();
        assertThat(proxy.equals(proxy)).isTrue();
        assertThat(proxy.equals(null)).isFalse();
        assertThat(proxy.toString()).startsWith("com.palantir.leader.proxy.AwaitingLeadershipProxy@");
    }

    @Test
    @SuppressWarnings("SelfEquals")
    // We're asserting that calling .equals on a proxy does not redirect the .equals call to the instance its being proxied.
    public void shouldAllowObjectMethodsWhenNotLeading() throws Exception {
        Runnable mockRunnable = mock(Runnable.class);
        Supplier<Runnable> delegateSupplier = Suppliers.ofInstance(mockRunnable);
        LeaderElectionService mockLeader = mock(LeaderElectionService.class);

        when(mockLeader.getSuspectedLeaderInMemory()).thenReturn(Optional.empty());
        when(mockLeader.isStillLeading(any(LeaderElectionService.LeadershipToken.class)))
                .thenReturn(LeaderElectionService.StillLeadingStatus.NOT_LEADING);

        Runnable proxy = AwaitingLeadershipProxy.newProxyInstance(Runnable.class, delegateSupplier, mockLeader);

        assertThat(proxy.hashCode()).isNotNull();
        assertThat(proxy.equals(proxy)).isTrue();
        assertThat(proxy.equals(null)).isFalse();
        assertThat(proxy.toString()).startsWith("com.palantir.leader.proxy.AwaitingLeadershipProxy@");
    }

    @Test
    public void shouldMapInterruptedExceptionToNCLEIfLeadingStatusChanges() throws Exception {
        Future callToProxy = makeARequestThatThrowsAndCauseLeadershipLoss(new InterruptedException(TEST_MESSAGE));

        assertThat(catchThrowable(callToProxy::get).getCause())
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("received an interrupt due to leader election.")
                .hasCauseExactlyInstanceOf(InterruptedException.class)
                .hasStackTraceContaining(TEST_MESSAGE);
    }

    @Test
    public void shouldNotMapOtherExceptionToNCLEIfLeadingStatusChanges() throws InterruptedException {
        Future callToProxy = makeARequestThatThrowsAndCauseLeadershipLoss(new IOException(TEST_MESSAGE));

        assertThat(catchThrowable(callToProxy::get).getCause())
                .isInstanceOf(IOException.class)
                .hasStackTraceContaining(TEST_MESSAGE);
    }

    @Test
    public void shouldNotMapInterruptedExceptionToNCLEIfLeadingStatusDoesntChange() throws InterruptedException {
        //Always leading
        Callable proxy = getCallableProxy(
                new InterruptedException(TEST_MESSAGE),
                LeaderElectionService.StillLeadingStatus.LEADING,
                LeaderElectionService.StillLeadingStatus.LEADING);

        Future callToProxy = asyncCallToProxy(proxy);

        assertThatThrownBy(proxy::call)
                .isInstanceOf(InterruptedException.class)
                .hasMessage(TEST_MESSAGE);

        assertThat(catchThrowable(callToProxy::get).getCause())
                .isInstanceOf(InterruptedException.class)
                .hasMessage(TEST_MESSAGE);
    }

    private Future makeARequestThatThrowsAndCauseLeadershipLoss(Exception ex) throws InterruptedException {
        //leadership lost while request in flight
        Callable proxy = getCallableProxy(
                ex,
                LeaderElectionService.StillLeadingStatus.LEADING,
                LeaderElectionService.StillLeadingStatus.NOT_LEADING);

        Future callToProxy = asyncCallToProxy(proxy);

        assertThatThrownBy(proxy::call).isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("method invoked on a non-leader (leadership lost)");
        return callToProxy;
    }

    private Callable getCallableProxy(
            Exception ex,
            LeaderElectionService.StillLeadingStatus status1,
            LeaderElectionService.StillLeadingStatus status2) throws InterruptedException {

        Callable runnableWithInterruptedException = () -> {
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
            throw ex;
        };

        Supplier<Callable> delegateSupplier = Suppliers.ofInstance(
                runnableWithInterruptedException);
        LeaderElectionService mockLeaderService = mock(LeaderElectionService.class);
        setUpTheLeaderElectionService(mockLeaderService, status1, status2);

        Callable proxy = AwaitingLeadershipProxy.newProxyInstance(
                Callable.class, delegateSupplier, mockLeaderService);

        //waiting for trytoGainLeadership
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        return proxy;
    }

    private Future asyncCallToProxy(Callable proxy) {
        Future submit = Executors.newSingleThreadExecutor().submit(proxy);
        //waiting for the leadership checks to complete so that it believes its the leader when method is invoked.
        Uninterruptibles.sleepUninterruptibly(2, TimeUnit.SECONDS);
        return submit;
    }

    private void setUpTheLeaderElectionService(
            LeaderElectionService mockLeaderService,
            LeaderElectionService.StillLeadingStatus status1,
            LeaderElectionService.StillLeadingStatus status2)
            throws InterruptedException {
        LeaderElectionService.LeadershipToken leadershipToken = mock(PaxosLeadershipToken.class);
        when(mockLeaderService.blockOnBecomingLeader()).thenReturn(getToken(status1, leadershipToken), getToken(status2, leadershipToken));
        when(mockLeaderService.getSuspectedLeaderInMemory()).thenReturn(Optional.empty());
        when(mockLeaderService.isStillLeading(leadershipToken)).thenReturn(status1, status2);
    }

    private LeaderElectionService.LeadershipToken getToken(LeaderElectionService.StillLeadingStatus status,
            LeaderElectionService.LeadershipToken leadershipToken) {
        return (status == LeaderElectionService.StillLeadingStatus.LEADING) ? leadershipToken : null;
    }
}
