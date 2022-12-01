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

package com.palantir.atlasdb.transaction.impl.expectations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterators;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.logsafe.Preconditions;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.function.ToLongFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class TrackingRowColumnRangeIteratorTest {
    private static final Entry<Cell, Value> ENTRY_1 =
            new SimpleImmutableEntry<>(createCellWithSize(10), createValueWithSize(10));
    private static final Entry<Cell, Value> ENTRY_2 =
            new SimpleImmutableEntry<>(createCellWithSize(20), createValueWithSize(20));
    private static final Entry<Cell, Value> ENTRY_3 =
            new SimpleImmutableEntry<>(createCellWithSize(30), createValueWithSize(30));

    @Mock
    private BytesReadTracker tracker;

    @Mock
    private ToLongFunction<Entry<Cell, Value>> measurer;

    @Test
    public void trackingIteratorDelegatesNext() {
        RowColumnRangeIterator trackingIterator = createTrackingIterator(Iterators.forArray(ENTRY_1, ENTRY_2, ENTRY_3));
        assertThat(trackingIterator).toIterable().containsExactly(ENTRY_1, ENTRY_2, ENTRY_3);
    }

    @Test
    public void trackingIteratorTracksAndMeasuresInDelegateIteratorOrder() {
        when(measurer.applyAsLong(ENTRY_1)).thenReturn(1L);
        when(measurer.applyAsLong(ENTRY_2)).thenReturn(2L);
        when(measurer.applyAsLong(ENTRY_3)).thenReturn(3L);

        RowColumnRangeIterator trackingIterator = createTrackingIterator(Iterators.forArray(ENTRY_1, ENTRY_2, ENTRY_3));
        trackingIterator.forEachRemaining(_unused -> {});

        InOrder inOrder = inOrder(tracker);
        inOrder.verify(tracker).record(1L);
        inOrder.verify(tracker).record(2L);
        inOrder.verify(tracker).record(3L);
        verifyNoMoreInteractions(tracker);
    }

    @Test
    public void trackingIteratorForwardsValuesDespiteExceptionAtMeasurement() {
        when(measurer.applyAsLong(ENTRY_1)).thenThrow(RuntimeException.class);
        RowColumnRangeIterator trackingIterator = createTrackingIterator(Iterators.singletonIterator(ENTRY_1));
        assertThat(trackingIterator).toIterable().containsExactly(ENTRY_1);
    }

    @Test
    public void trackingIteratorForwardsValuesDespiteExceptionAtConsumption() {
        doThrow(RuntimeException.class).when(tracker).record(anyLong());
        RowColumnRangeIterator trackingIterator = createTrackingIterator(Iterators.singletonIterator(ENTRY_1));
        assertThat(trackingIterator).toIterable().containsExactly(ENTRY_1);
    }

    public RowColumnRangeIterator createTrackingIterator(Iterator<Entry<Cell, Value>> delegate) {
        return new TrackingRowColumnRangeIterator(new LocalRowColumnRangeIterator(delegate), tracker, measurer);
    }

    private static Cell createCellWithSize(int size) {
        Preconditions.checkArgument(size >= 2, "size should be at least 2");
        return Cell.create(new byte[size / 2], new byte[size - (size / 2)]);
    }

    private static Value createValueWithSize(int size) {
        Preconditions.checkArgument(size >= Long.BYTES, "size should be at least the number of bytes in one long");
        return Value.create(new byte[size - Long.BYTES], Value.INVALID_VALUE_TIMESTAMP);
    }
}
