/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.common.api.annotations.ReviewedRestrictedApiUsage;
import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import com.palantir.lock.LockClient;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimestampLeaseResult;
import com.palantir.lock.v2.TimestampLeaseResults;
import com.palantir.logsafe.Preconditions;
import com.palantir.timestamp.InMemoryTimestampService;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import one.util.streamex.StreamEx;
import org.assertj.core.api.MapAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public final class LegacyTimelockServiceIntegrationTest {

    private static final LockClient LOCK_CLIENT = LockClient.of("foo");
    private static final TimestampLeaseName TIMESTAMP_LEASE_NAME1 = TimestampLeaseName.of("lease1");
    private static final TimestampLeaseName TIMESTAMP_LEASE_NAME2 = TimestampLeaseName.of("lease2");

    private LockServiceImpl lockService;
    private LegacyTimelockService timelock;

    @BeforeEach
    public void beforeEach() {
        lockService = LockServiceImpl.create();
        timelock = new LegacyTimelockService(new InMemoryTimestampService(), lockService, LOCK_CLIENT);
    }

    @AfterEach
    public void afterEach() {
        lockService.close();
    }

    @Test
    public void canAcquireAndReleaseMultipleLeases() {
        TestLease lease1 = acquire(TIMESTAMP_LEASE_NAME1, TIMESTAMP_LEASE_NAME2);
        TestLease lease2 = acquire(TIMESTAMP_LEASE_NAME1);
        TestLease lease3 = acquire(TIMESTAMP_LEASE_NAME2);
        TestLease lease4 = acquire(TIMESTAMP_LEASE_NAME1, TIMESTAMP_LEASE_NAME2);
        assertThat(lease1.minLeasedTimestamp())
                .isEqualTo(lease2.minLeasedTimestamp())
                .isEqualTo(lease3.minLeasedTimestamp())
                .isEqualTo(lease4.minLeasedTimestamp());
        assertThatMinLeaseTimestamps(TIMESTAMP_LEASE_NAME1, TIMESTAMP_LEASE_NAME2)
                .containsEntry(TIMESTAMP_LEASE_NAME1, lease1.minLeasedTimestamp())
                .containsEntry(TIMESTAMP_LEASE_NAME2, lease1.minLeasedTimestamp());

        // Release middle leases
        lease2.unlock();
        assertThatMinLeaseTimestamps(TIMESTAMP_LEASE_NAME1, TIMESTAMP_LEASE_NAME2)
                .containsEntry(TIMESTAMP_LEASE_NAME1, lease1.minLeasedTimestamp())
                .containsEntry(TIMESTAMP_LEASE_NAME2, lease1.minLeasedTimestamp());
        lease3.unlock();
        assertThatMinLeaseTimestamps(TIMESTAMP_LEASE_NAME1, TIMESTAMP_LEASE_NAME2)
                .containsEntry(TIMESTAMP_LEASE_NAME1, lease1.minLeasedTimestamp())
                .containsEntry(TIMESTAMP_LEASE_NAME2, lease1.minLeasedTimestamp());

        // Release oldest lease
        lease1.unlock();

        // TODO(jakubk): This is awkward because we don't have the leased timestamps in the response.
        MapAssert<TimestampLeaseName, Long> timestampsAssert =
                assertThatMinLeaseTimestamps(TIMESTAMP_LEASE_NAME1, TIMESTAMP_LEASE_NAME2);
        timestampsAssert.extractingByKey(TIMESTAMP_LEASE_NAME1).satisfies(minLeaseTimestamp -> assertThat(
                        minLeaseTimestamp)
                .isGreaterThan(lease4.minLeasedTimestamp()));
        timestampsAssert.extractingByKey(TIMESTAMP_LEASE_NAME2).satisfies(minLeaseTimestamp -> assertThat(
                        minLeaseTimestamp)
                .isGreaterThan(lease4.minLeasedTimestamp()));
    }

    @ReviewedRestrictedApiUsage
    private TestLease acquire(TimestampLeaseName... leases) {
        Map<TimestampLeaseName, Integer> request = StreamEx.of(leases)
                .mapToEntry(_ignored -> ThreadLocalRandom.current().nextInt(1, 10))
                .toMap();
        TimestampLeaseResults timestampLeaseResults = timelock.acquireTimestampLeases(request);
        return createTestLease(request, timestampLeaseResults);
    }

    private MapAssert<TimestampLeaseName, Long> assertThatMinLeaseTimestamps(TimestampLeaseName... expectedKeys) {
        return assertThat(getMinLeasedTimestamps(Set.of(expectedKeys))).containsOnlyKeys(Set.of(expectedKeys));
    }

    @ReviewedRestrictedApiUsage
    private Map<TimestampLeaseName, Long> getMinLeasedTimestamps(Set<TimestampLeaseName> timestampNames) {
        return timelock.getMinLeasedTimestamps(timestampNames);
    }

    private TestLease createTestLease(
            Map<TimestampLeaseName, Integer> request, TimestampLeaseResults timestampLeaseResults) {
        assertThat(timestampLeaseResults.results()).containsOnlyKeys(request.keySet());
        request.forEach((leaseName, numTimestamps) -> assertThat(timestampLeaseResults.results())
                .extractingByKey(leaseName)
                .satisfies(result -> assertThatSupplierHasExactly(result, numTimestamps)));
        Set<Long> minLeasedTimestamps = timestampLeaseResults.results().values().stream()
                .map(TimestampLeaseResult::minLeasedTimestamp)
                .collect(Collectors.toSet());
        assertThat(minLeasedTimestamps).hasSize(1);
        return new TestLease(Iterables.getOnlyElement(minLeasedTimestamps), timestampLeaseResults.lock());
    }

    private static void assertThatSupplierHasExactly(TimestampLeaseResult result, int minNumTimestamps) {
        LongStream.range(0, minNumTimestamps)
                .forEach(_ignore -> Preconditions.checkState(
                        result.freshTimestampsSupplier().getAsLong() > 0));
        assertThatThrownBy(result.freshTimestampsSupplier()::getAsLong).isInstanceOf(RuntimeException.class);
    }

    private final class TestLease {

        private final long minLeasedTimestamp;
        private final LockToken lock;

        private TestLease(long minLeasedTimestamp, LockToken lock) {
            this.minLeasedTimestamp = minLeasedTimestamp;
            this.lock = lock;
        }

        public void unlock() {
            timelock.unlock(Set.of(lock));
        }

        public long minLeasedTimestamp() {
            return minLeasedTimestamp;
        }
    }
}
