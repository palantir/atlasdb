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

package com.palantir.lock.client;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.ImmutableTransactionUpdate;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionUpdate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

public final class BatchingCommitTimestampGetterTest {

    private static final LockWatchStateUpdate UPDATE_1 = LockWatchStateUpdate.success(
            UUID.randomUUID(),
            0,
            ImmutableList.of(
                    LockEvent.builder(ImmutableSet.of(StringLockDescriptor.of("lock")), LockToken.of(UUID.randomUUID()))
                            .build(0)));
    private static final LockWatchStateUpdate UPDATE_2 = LockWatchStateUpdate.success(
            UUID.randomUUID(),
            1,
            ImmutableList.of(LockEvent.builder(
                            ImmutableSet.of(StringLockDescriptor.of("lock1")), LockToken.of(UUID.randomUUID()))
                    .build(1)));
    private static final Optional<LockWatchVersion> IDENTIFIED_VERSION_1 = Optional.empty();
    private static final Optional<LockWatchVersion> IDENTIFIED_VERSION_2 =
            Optional.of(LockWatchVersion.of(UUID.randomUUID(), -1));

    private final LockLeaseService lockLeaseService = mock(LockLeaseService.class);
    private final LockWatchEventCache cache = mock(LockWatchEventCache.class);
    private final Consumer<List<BatchElement<BatchingCommitTimestampGetter.Request, Long>>> batchProcessor =
            BatchingCommitTimestampGetter.consumer(lockLeaseService, cache);

    @Test
    public void consumerFillsTheWholeBatch() {
        BatchingCommitTimestampGetter.Request request1 = request(1, UUID.randomUUID());
        BatchingCommitTimestampGetter.Request request2 = request(2, UUID.randomUUID());
        BatchingCommitTimestampGetter.Request request3 = request(3, UUID.randomUUID());
        BatchingCommitTimestampGetter.Request request4 = request(4, UUID.randomUUID());

        when(cache.lastKnownVersion()).thenReturn(IDENTIFIED_VERSION_1).thenReturn(IDENTIFIED_VERSION_2);
        whenGetCommitTimestamps(IDENTIFIED_VERSION_1, 4, 5, 6, UPDATE_1);
        whenGetCommitTimestamps(IDENTIFIED_VERSION_2, 2, 7, 8, UPDATE_2);

        assertThat(processBatch(request1, request2, request3, request4)).containsExactly(5L, 6L, 7L, 8L);

        InOrder inOrder = Mockito.inOrder(lockLeaseService, cache);
        inOrder.verify(lockLeaseService).getCommitTimestamps(IDENTIFIED_VERSION_1, 4);
        inOrder.verify(cache)
                .processGetCommitTimestampsUpdate(
                        eq(ImmutableList.of(transactionUpdate(request1, 5), transactionUpdate(request2, 6))),
                        eq(UPDATE_1));
        inOrder.verify(lockLeaseService).getCommitTimestamps(IDENTIFIED_VERSION_2, 2);
        inOrder.verify(cache)
                .processGetCommitTimestampsUpdate(
                        eq(ImmutableList.of(transactionUpdate(request3, 7), transactionUpdate(request4, 8))),
                        eq(UPDATE_2));
    }

    private void whenGetCommitTimestamps(
            Optional<LockWatchVersion> maybeVersion, int count, int start, int end, LockWatchStateUpdate update) {
        when(lockLeaseService.getCommitTimestamps(maybeVersion, count))
                .thenReturn(GetCommitTimestampsResponse.builder()
                        .inclusiveLower(start)
                        .inclusiveUpper(end)
                        .lockWatchUpdate(update)
                        .build());
    }

    private List<Long> processBatch(BatchingCommitTimestampGetter.Request... requests) {
        List<BatchElement<BatchingCommitTimestampGetter.Request, Long>> elements = Arrays.stream(requests)
                .map(request -> ImmutableTestBatchElement.<BatchingCommitTimestampGetter.Request, Long>builder()
                        .argument(request)
                        .result(new DisruptorAutobatcher.DisruptorFuture<>("test"))
                        .build())
                .collect(toList());
        batchProcessor.accept(elements);
        return Futures.getUnchecked(Futures.allAsList(Lists.transform(elements, BatchElement::result)));
    }

    private BatchingCommitTimestampGetter.Request request(long startTs, UUID lockToken) {
        return ImmutableRequest.builder()
                .startTs(startTs)
                .commitLocksToken(LockToken.of(lockToken))
                .build();
    }

    private TransactionUpdate transactionUpdate(BatchingCommitTimestampGetter.Request request, long commitTs) {
        return ImmutableTransactionUpdate.builder()
                .startTs(request.startTs())
                .commitTs(commitTs)
                .writesToken(request.commitLocksToken())
                .build();
    }
}
