/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraCredentialsConfig;
import com.palantir.atlasdb.cassandra.ImmutableCassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.ImmutableDefaultConfig;
import com.palantir.common.base.FunctionCheckedException;

public class BlacklistTest {
    private static final InetSocketAddress ADDRESS_1 = InetSocketAddress.createUnresolved("NW16XE", 123);
    private static final InetSocketAddress ADDRESS_2 = InetSocketAddress.createUnresolved("SW1A2AA", 1234);
    private static final InetSocketAddress ADDRESS_3 = InetSocketAddress.createUnresolved("SE17PB", 12345);

    private static final Duration ONE_SECOND = Duration.ofSeconds(1);

    private static final CassandraKeyValueServiceConfig CONFIG = ImmutableCassandraKeyValueServiceConfig.builder()
            .servers(ImmutableDefaultConfig.builder().addThriftHosts(ADDRESS_1).build())
            .credentials(ImmutableCassandraCredentialsConfig.builder().username("a").password("b").build())
            .replicationFactor(1)
            .unresponsiveHostBackoffTimeSeconds(1)
            .build();

    private final AtomicLong time = new AtomicLong();
    private final Clock clock = mock(Clock.class);

    private final CassandraClientPoolingContainer goodContainer = mock(CassandraClientPoolingContainer.class);
    private final CassandraClientPoolingContainer badContainer = mock(CassandraClientPoolingContainer.class);

    private final Blacklist blacklist = new Blacklist(CONFIG, clock);

    @Before
    @SuppressWarnings("unchecked") // Mock type is correct
    public void setUp() {
        when(clock.millis()).thenAnswer(invocation -> time.addAndGet(ONE_SECOND.toMillis() + 1));
        when(badContainer.runWithPooledResource(any(FunctionCheckedException.class))).thenThrow(new RuntimeException());
        when(badContainer.getHost()).thenReturn(ADDRESS_1);
    }

    @Test
    public void canAddHostToBlacklist() {
        blacklist.add(ADDRESS_1);

        assertThat(blacklist.contains(ADDRESS_1)).isTrue();
        assertThat(blacklist.contains(ADDRESS_2)).isFalse();
    }

    @Test
    public void doesNotRemoveHostFromBlacklistIfTimeHasNotElapsedYet() {
        when(clock.millis()).thenReturn(42L);

        blacklist.add(ADDRESS_1);
        blacklist.checkAndUpdate(ImmutableMap.of(ADDRESS_1, goodContainer));

        assertThat(blacklist.contains(ADDRESS_1)).isTrue();
    }

    @Test
    public void doesNotRemoveHostFromBlacklistIfTimeHasElapsedAndNodeUnhealthy() {
        blacklist.add(ADDRESS_1);
        blacklist.checkAndUpdate(ImmutableMap.of(ADDRESS_1, badContainer));

        assertThat(blacklist.contains(ADDRESS_1)).isTrue();
    }

    @Test
    public void removesHostFromBlacklistIfTimeHasElapsedAndNodeHealthy() {
        blacklist.add(ADDRESS_1);
        blacklist.checkAndUpdate(ImmutableMap.of(ADDRESS_1, goodContainer));

        assertThat(blacklist.contains(ADDRESS_1)).isFalse();
    }

    @Test
    public void removesHostsFromBlacklistIfUnknown() {
        blacklist.add(ADDRESS_2);
        blacklist.checkAndUpdate(ImmutableMap.of(ADDRESS_1, goodContainer));

        assertThat(blacklist.contains(ADDRESS_2)).isFalse();
    }

    @Test
    public void handlesDifferentStatusUpdatesAsBatch() {
        blacklist.add(ADDRESS_1);
        blacklist.add(ADDRESS_2);
        blacklist.add(ADDRESS_3);
        blacklist.checkAndUpdate(ImmutableMap.of(ADDRESS_1, goodContainer, ADDRESS_2, badContainer));

        assertThat(blacklist.contains(ADDRESS_1)).isFalse();
        assertThat(blacklist.contains(ADDRESS_2)).isTrue();
        assertThat(blacklist.contains(ADDRESS_3)).isFalse();
    }
}
