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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.leader.LeaderElectionService;

public class AwaitingLeadershipProxyTest {

    @Test
    @SuppressWarnings("SelfEquals") // We're asserting that calling .equals on a proxy does not redirect the .equals call to the instance its being proxied.
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
    @SuppressWarnings("SelfEquals") // We're asserting that calling .equals on a proxy does not redirect the .equals call to the instance its being proxied.
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
}
