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

package com.palantir.lock.client;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.ImmutablePartitionedTimestamps;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.v2.StartIdentifiedAtlasDbTransactionResponse;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchStateUpdate;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

public class StartTransactionsUtils {
    private static final int NUM_PARTITIONS = 16;
    private static final LockImmutableTimestampResponse IMMUTABLE_TS_RESPONSE =
            LockImmutableTimestampResponse.of(1L, LockToken.of(UUID.randomUUID()));
    private static final Lease LEASE =
            Lease.of(LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(1L)), Duration.ofSeconds(1L));
    static final LockWatchStateUpdate UPDATE = LockWatchStateUpdate.success(
            UUID.randomUUID(),
            1,
            ImmutableList.of(
                    LockEvent.builder(ImmutableSet.of(StringLockDescriptor.of("lock")), LockToken.of(UUID.randomUUID()))
                            .build(0)));

    static void assertThatStartTransactionResponsesAreUnique(
            List<? extends StartIdentifiedAtlasDbTransactionResponse> responses) {
        assertThat(responses)
                .as("Each response should have a different immutable ts lock token")
                .extracting(response -> response.immutableTimestamp().getLock().getRequestId())
                .doesNotHaveDuplicates();

        assertThat(responses)
                .as("Each response should have a different start timestamp")
                .extracting(response -> response.startTimestampAndPartition().timestamp())
                .doesNotHaveDuplicates();
    }

    static void assertDerivableFromBatchedResponse(
            StartIdentifiedAtlasDbTransactionResponse startTransactionResponse,
            ConjureStartTransactionsResponse batchedStartTransactionResponse) {

        // Todo Snanda - assertion has been relaxed
        assertThat(startTransactionResponse.immutableTimestamp().getLock())
                .as("Should have a lock token share referencing to immutable ts lock token")
                .isInstanceOf(LockTokenShare.class);

        assertThat(startTransactionResponse.immutableTimestamp().getImmutableTimestamp())
                .as("Should have same immutable timestamp")
                .isEqualTo(
                        batchedStartTransactionResponse.getImmutableTimestamp().getImmutableTimestamp());

        assertThat(startTransactionResponse.startTimestampAndPartition().partition())
                .as("Should have same partition value")
                .isEqualTo(batchedStartTransactionResponse.getTimestamps().partition());

        assertThat(batchedStartTransactionResponse.getTimestamps().stream())
                .as("Start timestamp should be contained by batched response")
                .contains(startTransactionResponse.startTimestampAndPartition().timestamp());
    }

    static ConjureStartTransactionsResponse getStartTransactionResponse(long lowestStartTs, int count) {
        return ConjureStartTransactionsResponse.builder()
                .immutableTimestamp(IMMUTABLE_TS_RESPONSE)
                .timestamps(getPartitionedTimestamps(lowestStartTs, count))
                .lease(LEASE)
                .lockWatchUpdate(UPDATE)
                .build();
    }

    private static PartitionedTimestamps getPartitionedTimestamps(long startTs, int count) {
        return ImmutablePartitionedTimestamps.builder()
                .start(startTs)
                .count(count)
                .interval(NUM_PARTITIONS)
                .build();
    }
}
