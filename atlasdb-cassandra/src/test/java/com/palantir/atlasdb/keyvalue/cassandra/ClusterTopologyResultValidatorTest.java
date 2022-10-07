/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class ClusterTopologyResultValidatorTest {

    @Test
    public void validateNewlyAddedHosts_returnsEmptyWhenGivenNoServers() {
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(ImmutableSet.of(), ImmutableMap.of()))
                .isEmpty();
    }

    @Test
    public void validateNewlyAddedHosts_throwsWhenAllHostsDoesNotContainNewHosts() {
        assertThatThrownBy(() -> ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(
                        ImmutableSet.of(createCassandraServer("foo")), ImmutableMap.of()))
                .isInstanceOf(SafeIllegalArgumentException.class);
    }

    @Test
    public void validateNewlyAddedHosts_returnsEmpty_whenOnlyNewServers_andAllMatch() {
        assertThat(ClusterTopologyValidator.getNewHostsWithInconsistentTopologies(ImmutableSet.of(), ImmutableMap.of()))
                .isEmpty();
    }

    public static CassandraServer createCassandraServer(String hostname) {
        return CassandraServer.of(hostname, mock(InetSocketAddress.class));
    }
}
