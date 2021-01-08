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
package com.palantir.atlasdb.sweep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.service.TransactionService;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;

public class CommitTsCacheTest {
    private static final Long VALID_START_TIMESTAMP = 100L;
    private static final Long VALID_COMMIT_TIMESTAMP = 200L;
    private static final Long ROLLBACK_TIMESTAMP = TransactionConstants.FAILED_COMMIT_TS;
    private static final Long NO_TIMESTAMP = null;

    private final TransactionService mockTransactionService = mock(TransactionService.class);
    private final CommitTsCache loader = CommitTsCache.create(mockTransactionService);

    @Test
    public void loadShouldReturnTheValidTimestamp() throws Exception {
        when(mockTransactionService.get(VALID_START_TIMESTAMP)).thenReturn(VALID_COMMIT_TIMESTAMP);

        assertThat(loader.load(VALID_START_TIMESTAMP)).isEqualTo(VALID_COMMIT_TIMESTAMP);
    }

    @Test
    public void loadShouldPutRollbackIfCommitTsIsNull() throws Exception {
        AtomicLong answerCount = new AtomicLong();

        doAnswer(invocation -> answerCount.get() > 0 ? ROLLBACK_TIMESTAMP : NO_TIMESTAMP)
                .when(mockTransactionService)
                .get(VALID_START_TIMESTAMP);

        doAnswer(invocation -> {
                    answerCount.set(1);
                    return null;
                })
                .when(mockTransactionService)
                .putUnlessExists(VALID_START_TIMESTAMP, ROLLBACK_TIMESTAMP);

        assertThat(loader.load(VALID_START_TIMESTAMP)).isEqualTo(ROLLBACK_TIMESTAMP);

        verify(mockTransactionService).putUnlessExists(VALID_START_TIMESTAMP, ROLLBACK_TIMESTAMP);
    }

    @Test
    public void loadShouldContinueIfKeyAlreadyExistsIsThrown() throws Exception {
        AtomicLong answerCount = new AtomicLong();

        doAnswer(invocation -> answerCount.get() > 0 ? VALID_COMMIT_TIMESTAMP : NO_TIMESTAMP)
                .when(mockTransactionService)
                .get(VALID_START_TIMESTAMP);

        doAnswer(invocation -> {
                    answerCount.set(1);
                    throw new KeyAlreadyExistsException("Already exists");
                })
                .when(mockTransactionService)
                .putUnlessExists(VALID_START_TIMESTAMP, ROLLBACK_TIMESTAMP);

        assertThat(loader.load(VALID_START_TIMESTAMP)).isEqualTo(VALID_COMMIT_TIMESTAMP);

        verify(mockTransactionService).putUnlessExists(VALID_START_TIMESTAMP, ROLLBACK_TIMESTAMP);
    }

    @Test
    public void loadShouldThrowIfANullIsToBeReturned() throws Exception {
        doAnswer(invocation -> NO_TIMESTAMP).when(mockTransactionService).get(VALID_START_TIMESTAMP);

        assertThat(loader.load(VALID_START_TIMESTAMP)).isEqualTo(ROLLBACK_TIMESTAMP);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void warmingCacheShouldNotPlaceUndueLoadOnTransactionService() throws Exception {
        long valuesToInsert = 1_000_000;

        doAnswer(invocation -> {
                    Collection<Long> timestamps = (Collection<Long>) invocation.getArguments()[0];
                    if (timestamps.size() > AtlasDbConstants.TRANSACTION_TIMESTAMP_LOAD_BATCH_LIMIT) {
                        fail("Requested more timestamps in a batch than is reasonable!");
                    }
                    return timestamps.stream().collect(Collectors.toMap(n -> n, n -> n));
                })
                .when(mockTransactionService)
                .get(any());

        Set<Long> initialTimestamps =
                LongStream.range(0, valuesToInsert).boxed().collect(Collectors.toSet());

        loader.loadBatch(initialTimestamps);
        assertThat(loader.load(valuesToInsert - 1)).isEqualTo(valuesToInsert - 1);
    }

    @Test
    public void onlyRequestNonCachedTimestamps() throws Exception {
        Set<Long> initialTimestamps = LongStream.range(0L, 20L).boxed().collect(Collectors.toSet());
        doAnswer(invocation -> assertRequestedTimestampsAndMapIdentity(invocation, initialTimestamps))
                .when(mockTransactionService)
                .get(any());

        loader.loadBatch(initialTimestamps);
        assertThat(loader.load(19L)).isEqualTo(19L);

        Set<Long> moreTimestamps = LongStream.range(10L, 30L).boxed().collect(Collectors.toSet());
        doAnswer(invocation -> assertRequestedTimestampsAndMapIdentity(
                        invocation, Sets.difference(moreTimestamps, initialTimestamps)))
                .when(mockTransactionService)
                .get(any());

        loader.loadBatch(moreTimestamps);
        assertThat(loader.load(27L)).isEqualTo(27L);

        Set<Long> evenMoreTimestamps = LongStream.range(7L, 50L).boxed().collect(Collectors.toSet());
        doAnswer(invocation -> assertRequestedTimestampsAndMapIdentity(
                        invocation, Sets.difference(evenMoreTimestamps, Sets.union(initialTimestamps, moreTimestamps))))
                .when(mockTransactionService)
                .get(any());

        loader.loadBatch(evenMoreTimestamps);
        assertThat(loader.load(3L)).isEqualTo(3L);
        assertThat(loader.load(37L)).isEqualTo(37L);
        verify(mockTransactionService, times(3)).get(anyList());
        verifyNoMoreInteractions(mockTransactionService);
    }

    @Test
    public void loadIfCachedReturnsEmptyWhenNotCached() {
        when(mockTransactionService.get(VALID_START_TIMESTAMP)).thenReturn(VALID_COMMIT_TIMESTAMP);
        assertThat(loader.loadIfCached(VALID_START_TIMESTAMP)).isEmpty();
        verifyNoMoreInteractions(mockTransactionService);
    }

    @Test
    public void loadIfCachedReturnsWhenCached() {
        when(mockTransactionService.get(VALID_START_TIMESTAMP)).thenReturn(VALID_COMMIT_TIMESTAMP);
        when(mockTransactionService.get(VALID_START_TIMESTAMP + 1)).thenReturn(ROLLBACK_TIMESTAMP);
        assertThat(loader.load(VALID_START_TIMESTAMP)).isEqualTo(VALID_COMMIT_TIMESTAMP);
        assertThat(loader.load(VALID_START_TIMESTAMP + 1)).isEqualTo(ROLLBACK_TIMESTAMP);
        verify(mockTransactionService, times(2)).get(anyLong());

        assertThat(loader.loadIfCached(VALID_START_TIMESTAMP)).contains(VALID_COMMIT_TIMESTAMP);
        assertThat(loader.loadIfCached(VALID_START_TIMESTAMP + 1)).contains(ROLLBACK_TIMESTAMP);
        verifyNoMoreInteractions(mockTransactionService);
    }

    @Test
    public void loadIfCachedDoesNotAbortTransactionsAndCorrectlyGetsAbortedTransactions() {
        when(mockTransactionService.get(anyLong())).thenReturn(null);
        assertThat(loader.loadIfCached(VALID_START_TIMESTAMP)).isEmpty();
        assertThat(loader.load(VALID_START_TIMESTAMP)).isEqualTo(ROLLBACK_TIMESTAMP);
        assertThat(loader.loadIfCached(VALID_START_TIMESTAMP)).contains(ROLLBACK_TIMESTAMP);
    }

    @SuppressWarnings("unchecked")
    private Map<Long, Long> assertRequestedTimestampsAndMapIdentity(
            InvocationOnMock invocation, Collection<Long> expected) {
        Collection<Long> timestamps = (Collection<Long>) invocation.getArguments()[0];
        assertThat(timestamps).containsExactlyElementsOf(expected);
        return timestamps.stream().collect(Collectors.toMap(n -> n, n -> n));
    }
}
