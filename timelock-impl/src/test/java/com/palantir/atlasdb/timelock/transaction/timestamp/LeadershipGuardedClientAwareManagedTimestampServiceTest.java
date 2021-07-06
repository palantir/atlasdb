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

package com.palantir.atlasdb.timelock.transaction.timestamp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.leader.NotCurrentLeaderException;
import org.junit.Test;

public class LeadershipGuardedClientAwareManagedTimestampServiceTest {
    private final ClientAwareManagedTimestampService delegate = mock(ClientAwareManagedTimestampService.class);
    private final LeadershipGuardedClientAwareManagedTimestampService delegatingService =
            new LeadershipGuardedClientAwareManagedTimestampService(delegate);

    @Test
    public void passThroughCalls() {
        when(delegate.getFreshTimestamp()).thenReturn(88L);
        assertThat(delegatingService.getFreshTimestamp()).isEqualTo(88L);
        verify(delegate).getFreshTimestamp();
    }

    @Test
    public void doesNotReturnResultsAfterClosing() {
        when(delegate.getFreshTimestamp()).thenReturn(88L);
        delegatingService.close();
        assertThatThrownBy(delegatingService::getFreshTimestamp)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("Lost leadership elsewhere");

        // Maybe excessive, but I think reasonable
        verify(delegate).getFreshTimestamp();
    }

    @Test
    public void doNotReturnIfClosedAfterDelegateInvocationBegins() {
        when(delegate.getFreshTimestamp()).thenAnswer(invocation -> {
            delegatingService.close();
            return 42L;
        });
        assertThatThrownBy(delegatingService::getFreshTimestamp)
                .isInstanceOf(NotCurrentLeaderException.class)
                .hasMessage("Lost leadership elsewhere");
    }
}
