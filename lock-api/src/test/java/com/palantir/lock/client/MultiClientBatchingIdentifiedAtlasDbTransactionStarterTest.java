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

import static com.palantir.lock.client.MultiClientBatchingIdentifiedAtlasDbTransactionStarter.processBatch;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher.DisruptorFuture;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.LeaderTimes;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.common.streams.KeyedStream;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.client.MultiClientBatchingIdentifiedAtlasDbTransactionStarter.NamespacedStartTransactionsRequestParams;
import com.palantir.lock.client.MultiClientBatchingIdentifiedAtlasDbTransactionStarter.StartTransactionsRequestParams;
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
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;

public class MultiClientBatchingIdentifiedAtlasDbTransactionStarterTest {

    private static final int NUM_PARTITIONS = 16;
    private static final int PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL = 5;

    private static final LockImmutableTimestampResponse IMMUTABLE_TS_RESPONSE =
            LockImmutableTimestampResponse.of(1L, LockToken.of(UUID.randomUUID()));

    private static final Lease LEASE =
            Lease.of(LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(1L)), Duration.ofSeconds(1L));
    private static final LockWatchStateUpdate UPDATE = LockWatchStateUpdate.success(
            UUID.randomUUID(),
            1,
            ImmutableList.of(
                    LockEvent.builder(ImmutableSet.of(StringLockDescriptor.of("lock")), LockToken.of(UUID.randomUUID()))
                            .build(0)));

    @Test
    public void canServiceOneClient() {
        assertSanityOfResponse(
                getStartTransactionRequestsForClients(1, PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL - 1));
    }

    @Test
    public void canServiceOneClientWithMultipleServerCalls() {
        assertSanityOfResponse(
                getStartTransactionRequestsForClients(1, PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL * 27));
    }

    @Test
    public void canServiceMultipleClients() {
        int clientCount = 50;
        assertSanityOfResponse(getStartTransactionRequestsForClients(
                clientCount, (PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL - 1) * clientCount));
    }

    @Test
    public void canServiceMultipleClientsWithMultipleServerCalls() {
        int clientCount = 5;
        assertSanityOfResponse(getStartTransactionRequestsForClients(
                clientCount, (PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL + 1) * clientCount));
    }

    @Test
    public void canServiceOneRequestWithMultipleServerRequests() {
        ImmutableList<
                        BatchElement<
                                NamespacedStartTransactionsRequestParams,
                                List<StartIdentifiedAtlasDbTransactionResponse>>>
                batch = ImmutableList.of(BatchElement.of(
                NamespacedStartTransactionsRequestParams.of(
                        Namespace.of("Test_0"), StartTransactionsRequestParams.of(127, Optional::empty)),
                new DisruptorFuture<>("test")));
        assertSanityOfResponse(batch);
    }

    private void assertSanityOfResponse(
            List<
                            BatchElement<
                                    NamespacedStartTransactionsRequestParams,
                                    List<StartIdentifiedAtlasDbTransactionResponse>>>
                    requestsForClients) {
        processBatch(new MockMultiClientTimeLockService(), UUID.randomUUID(), requestsForClients);
        requestsForClients.stream().forEach(batchElement -> {
            DisruptorFuture<List<StartIdentifiedAtlasDbTransactionResponse>> resultFuture = batchElement.result();
            NamespacedStartTransactionsRequestParams requestParams = batchElement.argument();

            assertThat(resultFuture.isDone()).isTrue();
            assertThat(Futures.getUnchecked(resultFuture).size())
                    .isEqualTo(requestParams.params().numTransactions());
        });
    }

    private List<
                    BatchElement<
                            NamespacedStartTransactionsRequestParams, List<StartIdentifiedAtlasDbTransactionResponse>>>
            getStartTransactionRequestsForClients(int clientCount, int requestCount) {
        return IntStream.rangeClosed(1, requestCount)
                .mapToObj(ind -> BatchElement.of(
                        NamespacedStartTransactionsRequestParams.of(
                                Namespace.of("Test_" + (ind % clientCount)),
                                StartTransactionsRequestParams.of(1, Optional::empty)),
                        new DisruptorFuture<List<StartIdentifiedAtlasDbTransactionResponse>>("test")))
                .collect(Collectors.toList());
    }

    private static ConjureStartTransactionsResponse getStartTransactionResponse(long lowestStartTs, int count) {
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

    class MockMultiClientTimeLockService implements InternalMultiClientConjureTimelockService {

        @Override
        public LeaderTimes leaderTimes(Set<Namespace> namespaces) {
            return null;
        }

        @Override
        public Map<Namespace, ConjureStartTransactionsResponse> startTransactions(
                Map<Namespace, ConjureStartTransactionsRequest> requests) {
            return KeyedStream.stream(requests)
                    .map(request -> getStartTransactionResponse(
                            1, Math.min(PARTITIONED_TIMESTAMPS_LIMIT_PER_SERVER_CALL, request.getNumTransactions())))
                    .collectToMap();
        }
    }
}
