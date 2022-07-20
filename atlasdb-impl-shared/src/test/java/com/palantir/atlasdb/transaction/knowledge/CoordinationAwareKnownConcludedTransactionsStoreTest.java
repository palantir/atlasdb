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

package com.palantir.atlasdb.transaction.knowledge;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.internalschema.TimestampPartitioningMap;
import java.util.Optional;
import org.junit.Test;

public final class CoordinationAwareKnownConcludedTransactionsStoreTest {
    private final KnownConcludedTransactionsStore delegate = mock(KnownConcludedTransactionsStore.class);

    @Test
    public void coordinationStoreDelegatesGetToUnderlyingStore() {
        CoordinationAwareKnownConcludedTransactionsStore coordinationAwareStore = getDefaultCoordinationAwareStore();

        TimestampRangeSet expectedTimestampRangeSet = TimestampRangeSet.singleRange(Range.closedOpen(1L, 100L));
        when(delegate.get()).thenReturn(Optional.of(expectedTimestampRangeSet));

        assertThat(coordinationAwareStore.get()).hasValue(expectedTimestampRangeSet);
        verify(delegate).get();
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void doesNotSupplementIfRangeNotOnTransactions4() {
        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.closedOpen(1L, 100L), 1)
                .put(Range.atLeast(100L), 2)
                .build();
        CoordinationAwareKnownConcludedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));

        coordinationAwareStore.supplement(Range.closedOpen(100L, 200L));
        verifyNoMoreInteractions(delegate);
    }

    @Test
    public void supplementsWithRangeOnTransactions4() {
        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.closedOpen(1L, 10L), 1)
                .put(Range.closedOpen(10L, 20L), 2)
                .put(Range.closedOpen(20L, 30L), 3)
                .put(Range.atLeast(30L), 4)
                .build();
        CoordinationAwareKnownConcludedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));

        Range<Long> rangeToSupplement = Range.closedOpen(101L, 200L);
        coordinationAwareStore.supplement(rangeToSupplement);
        verify(delegate).supplement(ImmutableSet.of(rangeToSupplement));
    }

    @Test
    public void canSupplementsWithMultipleRangesOnTransactions4() {
        RangeMap<Long, Integer> rangeMap = ImmutableRangeMap.<Long, Integer>builder()
                .put(Range.closedOpen(1L, 100L), 1)
                .put(Range.closedOpen(100L, 200L), 4)
                .put(Range.closedOpen(200L, 300L), 3)
                .put(Range.atLeast(300L), 4)
                .build();
        CoordinationAwareKnownConcludedTransactionsStore coordinationAwareStore =
                getCoordinationAwareStore(TimestampPartitioningMap.of(rangeMap));

        Range<Long> rangeToSupplement = Range.closedOpen(100L, 400L);
        coordinationAwareStore.supplement(rangeToSupplement);
        verify(delegate).supplement(ImmutableSet.of(Range.closedOpen(100L, 200L), Range.closedOpen(300L, 400L)));
    }

    private CoordinationAwareKnownConcludedTransactionsStore getDefaultCoordinationAwareStore() {
        return new CoordinationAwareKnownConcludedTransactionsStore(
                _unused -> TimestampPartitioningMap.of(ImmutableRangeMap.of()), delegate);
    }

    private CoordinationAwareKnownConcludedTransactionsStore getCoordinationAwareStore(
            TimestampPartitioningMap<Integer> partitioningMap) {
        return new CoordinationAwareKnownConcludedTransactionsStore(_unused -> partitioningMap, delegate);
    }
}
