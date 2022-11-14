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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.logsafe.Preconditions;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.function.ToLongFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public final class TrackingRowColumnRangeIteratorTest {
    private static final Entry<Cell, Value> ENTRY =
            new SimpleImmutableEntry<>(createCellWithSize(10), createValueWithSize(10));
    private static final ImmutableMap<Cell, Value> VALUE_BY_CELL = ImmutableMap.of(
            createCellWithSize(10), createValueWithSize(10),
            createCellWithSize(20), createValueWithSize(20),
            createCellWithSize(30), createValueWithSize(30));

    @Mock
    private BytesReadTracker tracker;

    @Mock
    private ToLongFunction<Entry<Cell, Value>> measurer;

    @Test
    public void oneElementTrackingIteratorIsWiredCorrectly() {
        long measuredValue = 1L;
        when(measurer.applyAsLong(any())).thenReturn(measuredValue);
        RowColumnRangeIterator trackingIterator =
                createTrackingIterator(new LocalRowColumnRangeIterator(Iterators.singletonIterator(ENTRY)));

        assertThat(trackingIterator).toIterable().containsExactly(ENTRY);

        verify(measurer).applyAsLong(ENTRY);
        verify(tracker).record(measuredValue);
        verifyNoMoreInteractions(tracker, measurer);
    }

    @Test
    public void trackingIteratorDelegatesNext() {
        RowColumnRangeIterator trackingIterator = createTrackingIterator(
                new LocalRowColumnRangeIterator(VALUE_BY_CELL.entrySet().iterator()));
        assertThat(trackingIterator).toIterable().containsExactlyElementsOf(VALUE_BY_CELL.entrySet());
    }

    @Test
    public void trackingIteratorFeedsTracker() {
        when(measurer.applyAsLong(any())).thenReturn(1L).thenReturn(2L).thenReturn(3L);
        RowColumnRangeIterator trackingIterator = createTrackingIterator(new LocalRowColumnRangeIterator(
                VALUE_BY_CELL.entrySet().stream().iterator()));
        trackingIterator.forEachRemaining(_unused -> {});

        InOrder inOrder = inOrder(tracker);
        inOrder.verify(tracker).record(1L);
        inOrder.verify(tracker).record(2L);
        inOrder.verify(tracker).record(3L);
        verifyNoMoreInteractions(tracker);
    }

    @Test
    public void trackingIteratorForwardsValuesDespiteExceptionAtMeasurement() {
        when(measurer.applyAsLong(any())).thenThrow(RuntimeException.class);
        RowColumnRangeIterator trackingIterator =
                createTrackingIterator(new LocalRowColumnRangeIterator(Iterators.singletonIterator(ENTRY)));
        assertThat(trackingIterator).toIterable().containsExactly(ENTRY);
    }

    @Test
    public void trackingIteratorForwardsValuesDespiteExceptionAtConsumption() {
        doThrow(RuntimeException.class).when(tracker).record(anyLong());
        RowColumnRangeIterator trackingIterator =
                createTrackingIterator(new LocalRowColumnRangeIterator(Iterators.singletonIterator(ENTRY)));
        assertThat(trackingIterator).toIterable().containsExactly(ENTRY);
    }

    public RowColumnRangeIterator createTrackingIterator(RowColumnRangeIterator delegate) {
        return new TrackingRowColumnRangeIterator(delegate, tracker, measurer);
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
