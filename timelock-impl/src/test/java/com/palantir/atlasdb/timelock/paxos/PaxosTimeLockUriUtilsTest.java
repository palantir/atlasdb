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
package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.junit.Test;

public class PaxosTimeLockUriUtilsTest {
    private static final String ADDRESS_1 = "foo:1234";
    private static final String ADDRESS_2 = "bar:5678";
    private static final Set<String> ADDRESSES = ImmutableSet.of(ADDRESS_1, ADDRESS_2);

    private static final String CLIENT = "testClient";

    @Test
    public void canGetLeaderUris() {
        Set<String> leaderUris = PaxosTimeLockUriUtils.getLeaderPaxosUris(ADDRESSES);

        assertThat(leaderUris).containsExactlyInAnyOrder(
                String.join("/",
                        ADDRESS_1,
                        PaxosTimeLockConstants.INTERNAL_NAMESPACE,
                        PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE),
                String.join("/",
                        ADDRESS_2,
                        PaxosTimeLockConstants.INTERNAL_NAMESPACE,
                        PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE));
    }

    @Test
    public void canGetClientPaxosUris() {
        Set<String> clientPaxosUris = PaxosTimeLockUriUtils.getClientPaxosUris(ADDRESSES, CLIENT);

        assertThat(clientPaxosUris).containsExactlyInAnyOrder(
                String.join("/",
                        ADDRESS_1,
                        PaxosTimeLockConstants.INTERNAL_NAMESPACE,
                        PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE,
                        CLIENT),
                String.join("/",
                        ADDRESS_2,
                        PaxosTimeLockConstants.INTERNAL_NAMESPACE,
                        PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE,
                        CLIENT));
    }
}
