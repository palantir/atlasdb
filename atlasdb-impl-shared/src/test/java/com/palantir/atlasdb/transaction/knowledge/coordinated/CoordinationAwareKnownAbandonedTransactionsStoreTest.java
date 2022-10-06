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

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.ValueAndBound;
import com.palantir.atlasdb.internalschema.InternalSchemaMetadata;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;
import com.palantir.atlasdb.transaction.impl.TransactionConstants;
import com.palantir.atlasdb.transaction.knowledge.AbandonedTimestampStore;
import java.util.Optional;
import java.util.Set;
import org.junit.Test;

public final class CoordinationAwareKnownAbandonedTransactionsStoreTest {
    private final CoordinationService<InternalSchemaMetadata> coordinationService = mock(CoordinationService.class);

    private final AbandonedTimestampStore delegate = mock(AbandonedTimestampStore.class);

    @Test
    public void coordinationStoreDelegatesGetToUnderlyingStore() {
        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.atLeast(1L), TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION)
                .build();
        CoordinationAwareKnownAbandonedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));
        ImmutableSet<Long> abandonedTs = ImmutableSet.of(100L, 1487L, 247808L);
        coordinationAwareStore.addAbandonedTimestamps(abandonedTs);
        verify(delegate).markAbandoned(abandonedTs);
    }

    @Test
    public void doesNotSupplementIfRangeNotOnTransactions4() {
        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.closedOpen(1L, 100L), TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.atLeast(100L), TransactionConstants.TICKETS_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .build();
        CoordinationAwareKnownAbandonedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));

        coordinationAwareStore.addAbandonedTimestamps(ImmutableSet.of(1L, 100L, 200L, 400L));
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
        CoordinationAwareKnownAbandonedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));
        Set<Long> abandonedTs = ImmutableSet.of(1L, 10L, 20L, 25L, 30L, 300L);
        coordinationAwareStore.addAbandonedTimestamps(abandonedTs);
        verify(delegate).markAbandoned(ImmutableSet.of(30L, 300L));
    }

    @Test
    public void canSupplementWithMultipleRangesOnTransactions4() {
        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.closedOpen(1L, 100L), TransactionConstants.DIRECT_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.closedOpen(100L, 200L), TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.closedOpen(200L, 300L), TransactionConstants.TWO_STAGE_ENCODING_TRANSACTIONS_SCHEMA_VERSION)
                .put(Range.atLeast(300L), TransactionConstants.TTS_TRANSACTIONS_SCHEMA_VERSION)
                .build();

        CoordinationAwareKnownAbandonedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));
        Set<Long> abandonedTs = ImmutableSet.of(100L, 150L, 200L, 250L, 300L, 400L);
        coordinationAwareStore.addAbandonedTimestamps(abandonedTs);
        verify(delegate).markAbandoned(ImmutableSet.of(100L, 150L, 300L, 400L));
    }

    @Test
    public void ignoresUnknownSchemaVersions() {
        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.atLeast(1L), 5)
                .build();
        CoordinationAwareKnownAbandonedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));
        Set<Long> abandonedTs = ImmutableSet.of(1L, 3L, 5L, 7L);
        coordinationAwareStore.addAbandonedTimestamps(abandonedTs);
        verifyNoMoreInteractions(delegate);
    }

    private CoordinationAwareKnownAbandonedTransactionsStore getCoordinationAwareStore(
            TimestampPartitioningMap<Integer> timestampPartitioningMap) {
        when(coordinationService.getValueForTimestamp(anyLong()))
                .thenReturn(Optional.of(ValueAndBound.of(
                        InternalSchemaMetadata.builder()
                                .timestampToTransactionsTableSchemaVersion(timestampPartitioningMap)
                                .build(),
                        Long.MAX_VALUE)));
        return new CoordinationAwareKnownAbandonedTransactionsStore(coordinationService, delegate);
    }
}
