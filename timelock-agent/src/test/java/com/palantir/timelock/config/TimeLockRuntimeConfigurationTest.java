/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.timelock.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants;

public class TimeLockRuntimeConfigurationTest {
    @Test
    public void canCreateWithZeroClients() {
        ImmutableTimeLockRuntimeConfiguration.builder().build();
    }

    @Test
    public void canCreateWithClientsMatchingRegex() {
        String client1 = "test12345";
        String client2 = "-_-";
        String client3 = "___---___";

        ImmutableTimeLockRuntimeConfiguration.builder()
                .addClients(client1, client2, client3)
                .build();
    }

    @Test
    public void cannotCreateClientMatchingLeaderElectionNamespace() {
        assertThatThrownBy(() -> ImmutableTimeLockRuntimeConfiguration.builder()
                .addClients(PaxosTimeLockConstants.LEADER_ELECTION_NAMESPACE)
                .build()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void canCreateClientsMatchingPaxosNamespaces() {
        ImmutableTimeLockRuntimeConfiguration.builder()
                .addClients(PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE,
                        PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE)
                .build();
    }

    @Test
    public void canSpecifyPositiveLockLoggerTimeout() {
        ImmutableTimeLockRuntimeConfiguration.builder()
                .slowLockLogTriggerMillis(1L)
                .build();
    }

    @Test
    public void throwOnNegativeLeaderPingResponseWait() {
        assertThatThrownBy(() -> ImmutableTimeLockRuntimeConfiguration.builder()
                .slowLockLogTriggerMillis(-1L)
                .build()).isInstanceOf(IllegalStateException.class);
    }
}
