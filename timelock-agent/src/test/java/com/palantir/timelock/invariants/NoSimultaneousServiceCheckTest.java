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

package com.palantir.timelock.invariants;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.function.Consumer;

import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.timelock.paxos.Client;
import com.palantir.timelock.TimeLockStatus;
import com.palantir.timelock.paxos.HealthCheckDigest;
import com.palantir.timelock.paxos.ImmutableHealthCheckDigest;

@SuppressWarnings("unchecked") // Usage of mocks in conjunction with generics
public class NoSimultaneousServiceCheckTest {
    private static final Client CLIENT = Client.of("client");
    private static final Client CLIENT_2 = Client.of("client2");
    private static final Client CLIENT_3 = Client.of("client3");
    private static final HealthCheckDigest DIGEST = ImmutableHealthCheckDigest.builder()
            .putStatusesToClient(TimeLockStatus.MULTIPLE_LEADERS, CLIENT)
            .build();
    private static final HealthCheckDigest MULTISTATE_DIGEST = ImmutableHealthCheckDigest.builder()
            .putStatusesToClient(TimeLockStatus.MULTIPLE_LEADERS, CLIENT)
            .putStatusesToClient(TimeLockStatus.NO_LEADER, CLIENT_2)
            .putStatusesToClient(TimeLockStatus.ONE_LEADER, CLIENT_3)
            .build();

    private final TimeLockActivityChecker checker1 = mock(TimeLockActivityChecker.class);
    private final TimeLockActivityChecker checker2 = mock(TimeLockActivityChecker.class);
    private final Consumer<String> failureMechanism = mock(Consumer.class);
    private final NoSimultaneousServiceCheck noSimultaneousServiceCheck = new NoSimultaneousServiceCheck(
            ImmutableList.of(checker1, checker2), failureMechanism, MoreExecutors.newDirectExecutorService());

    @Test
    public void failureMechanismNotInvokedIfOneNodeIsTheLeader() {
        when(checker1.isThisNodeActivelyServingTimestampsForClient(CLIENT.value())).thenReturn(true);
        when(checker2.isThisNodeActivelyServingTimestampsForClient(CLIENT.value())).thenReturn(false);

        noSimultaneousServiceCheck.processHealthCheckDigest(DIGEST);
        verify(failureMechanism, never()).accept(anyString());
    }

    @Test
    public void failureMechanismNotInvokedIfNoNodeIsTheLeader() {
        when(checker1.isThisNodeActivelyServingTimestampsForClient(CLIENT.value())).thenReturn(false);
        when(checker2.isThisNodeActivelyServingTimestampsForClient(CLIENT.value())).thenReturn(false);

        noSimultaneousServiceCheck.processHealthCheckDigest(DIGEST);
        verify(failureMechanism, never()).accept(anyString());
    }

    @Test
    public void failureMechanismInvokedIfMultipleNodesConsistentlyClaimToBeTheLeader() {
        when(checker1.isThisNodeActivelyServingTimestampsForClient(CLIENT.value())).thenReturn(true);
        when(checker2.isThisNodeActivelyServingTimestampsForClient(CLIENT.value())).thenReturn(true);

        noSimultaneousServiceCheck.processHealthCheckDigest(DIGEST);
        verify(failureMechanism).accept(CLIENT.value());
    }

    @Test
    public void failureMechanismNotInvokedForPossibleLeadershipChanges() {
        when(checker1.isThisNodeActivelyServingTimestampsForClient(CLIENT.value()))
                .thenReturn(true)
                .thenReturn(true)
                .thenReturn(false);
        when(checker2.isThisNodeActivelyServingTimestampsForClient(CLIENT.value())).thenReturn(true);

        noSimultaneousServiceCheck.processHealthCheckDigest(DIGEST);
        verify(failureMechanism, never()).accept(anyString());
        verify(checker1, times(3)).isThisNodeActivelyServingTimestampsForClient(CLIENT.value());
        verify(checker2, times(3)).isThisNodeActivelyServingTimestampsForClient(CLIENT.value());
    }

    @Test
    public void onlyRunsChecksIfWeSuspectMultipleLeaders() {
        when(checker1.isThisNodeActivelyServingTimestampsForClient(anyString())).thenReturn(false);
        when(checker2.isThisNodeActivelyServingTimestampsForClient(anyString())).thenReturn(true);

        noSimultaneousServiceCheck.processHealthCheckDigest(MULTISTATE_DIGEST);
        verify(failureMechanism, never()).accept(anyString());
        verify(checker1).isThisNodeActivelyServingTimestampsForClient(CLIENT.value());
        verify(checker1, never()).isThisNodeActivelyServingTimestampsForClient(CLIENT_2.value());
        verify(checker1, never()).isThisNodeActivelyServingTimestampsForClient(CLIENT_3.value());

        verify(checker2).isThisNodeActivelyServingTimestampsForClient(CLIENT.value());
        verify(checker2, never()).isThisNodeActivelyServingTimestampsForClient(CLIENT_2.value());
        verify(checker2, never()).isThisNodeActivelyServingTimestampsForClient(CLIENT_3.value());
    }
}
