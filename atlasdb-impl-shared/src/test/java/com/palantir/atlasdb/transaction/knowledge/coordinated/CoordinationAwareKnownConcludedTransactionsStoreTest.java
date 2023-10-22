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

package com.palantir.atlasdb.transaction.knowledge.coordinated;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.knowledge.KnownConcludedTransactionsImpl;
import org.junit.jupiter.api.Test;

public final class CoordinationAwareKnownConcludedTransactionsStoreTest {
    private final KnownConcludedTransactionsImpl delegate = mock(KnownConcludedTransactionsImpl.class);

    @Test
    public void doesNotSupplementIfRangeNotOnTransactions4() {
        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.closedOpen(1L, 100L), TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.atLeast(100L), TransactionConstants.TICKETS_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .build();
        CoordinationAwareKnownConcludedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));

        coordinationAwareStore.addConcludedTimestamps(Range.closedOpen(100L, 200L));
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void canSupplementsWithRangeOnTransactions4() {
        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.closedOpen(1L, 10L), TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.closedOpen(10L, 20L), TransactionConstants.TICKETS_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.closedOpen(20L, 30L), TransactionConstants.TWO_STAGE_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.atLeast(30L), TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION)
                .build();
        CoordinationAwareKnownConcludedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));

        Range<Long> rangeToSupplement = Range.closedOpen(101L, 200L);
        coordinationAwareStore.addConcludedTimestamps(rangeToSupplement);
        verify(delegate).addConcludedTimestamps(ImmutableSet.of(rangeToSupplement));
    }

    @Test
    public void canSupplementWithMultipleRangesOnTransactions4() {
        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.closedOpen(1L, 100L), TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.closedOpen(100L, 200L), TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.closedOpen(200L, 300L), TransactionConstants.TWO_STAGE_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.atLeast(300L), TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION)
                .build();
        CoordinationAwareKnownConcludedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));

        Range<Long> rangeToSupplement = Range.closedOpen(100L, 400L);
        coordinationAwareStore.addConcludedTimestamps(rangeToSupplement);
        verify(delegate)
                .addConcludedTimestamps(ImmutableSet.of(Range.closedOpen(100L, 200L), Range.closedOpen(300L, 400L)));
    }

    @Test
    public void supplementDoesNotThrowWhenRangeDoesNotIntersect() {
        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.closedOpen(1L, 100L), TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.closedOpen(100L, 200L), TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.atLeast(200L), TransactionConstants.TWO_STAGE_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .build();
        CoordinationAwareKnownConcludedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));

        Range<Long> rangeToSupplement = Range.closedOpen(201L, 400L);
        assertThatCode(() -> coordinationAwareStore.addConcludedTimestamps(rangeToSupplement))
                .doesNotThrowAnyException();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void canSupplementForNewerSchemas() {
        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.atLeast(1L), 7)
                .build();
        CoordinationAwareKnownConcludedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));

        Range<Long> rangeToSupplement = Range.closedOpen(1L, 350L);
        assertThatCode(() -> coordinationAwareStore.addConcludedTimestamps(rangeToSupplement))
                .doesNotThrowAnyException();
        verify(delegate).addConcludedTimestamps(ImmutableSet.of(rangeToSupplement));
    }

    private CoordinationAwareKnownConcludedTransactionsStore getCoordinationAwareStore(
            TimestampPartitioningMap<Integer> partitioningMap) {
        return new CoordinationAwareKnownConcludedTransactionsStore(_unused -> partitioningMap, delegate);
    }
}
