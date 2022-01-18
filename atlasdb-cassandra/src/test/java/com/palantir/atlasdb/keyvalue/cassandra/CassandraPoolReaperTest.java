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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableSet;
import java.net.InetSocketAddress;
import org.junit.Test;

public class CassandraPoolReaperTest {
    private static final InetSocketAddress ADDRESS_1 = new InetSocketAddress(1);
    private static final InetSocketAddress ADDRESS_2 = new InetSocketAddress(2);
    private static final InetSocketAddress ADDRESS_3 = new InetSocketAddress(3);

    private static final int REQUIRED_CONSECUTIVE_REQUESTS = 3;

    private final CassandraPoolReaper reaper = new CassandraPoolReaper(REQUIRED_CONSECUTIVE_REQUESTS);

    @Test
    public void removesServerAfterConsecutiveRequests() {
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1))).isEmpty();
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1))).isEmpty();
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1))).containsExactly(ADDRESS_1);
    }

    @Test
    public void doesNotRemoveServerIfNotConsecutivelyRequested() {
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1))).isEmpty();
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1))).isEmpty();
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_2))).isEmpty();
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1)))
                .as("address 1 was not part of the previous request")
                .isEmpty();
    }

    @Test
    public void onlyRemovesConsecutivelyPresentServers() {
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1))).isEmpty();
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1, ADDRESS_2)))
                .isEmpty();
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1, ADDRESS_2, ADDRESS_3)))
                .containsExactly(ADDRESS_1);
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_2, ADDRESS_3)))
                .containsExactly(ADDRESS_2);
    }

    @Test
    public void continuesToRecommendServerForRemovalIfRepeatedlyRequested() {
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1))).isEmpty();
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1))).isEmpty();
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1))).containsExactly(ADDRESS_1);
        assertThat(reaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1))).containsExactly(ADDRESS_1);
    }

    @Test
    public void alwaysRecommendsServersToBeRemovedIfConfiguredWithLimitOne() {
        CassandraPoolReaper oneShotReaper = new CassandraPoolReaper(1);

        assertThat(oneShotReaper.computeServersToRemove(ImmutableSet.of())).isEmpty();
        assertThat(oneShotReaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1)))
                .containsExactly(ADDRESS_1);
        assertThat(oneShotReaper.computeServersToRemove(ImmutableSet.of(ADDRESS_2, ADDRESS_3)))
                .containsExactly(ADDRESS_2, ADDRESS_3);
        assertThat(oneShotReaper.computeServersToRemove(ImmutableSet.of(ADDRESS_1, ADDRESS_2, ADDRESS_3)))
                .containsExactly(ADDRESS_1, ADDRESS_2, ADDRESS_3);
    }
}
