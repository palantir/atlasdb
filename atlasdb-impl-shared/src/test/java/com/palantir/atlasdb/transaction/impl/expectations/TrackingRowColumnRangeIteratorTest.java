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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.logsafe.Preconditions;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import one.util.streamex.StreamEx;
import org.junit.Test;

public final class TrackingRowColumnRangeIteratorTest {
    private static final Entry<Cell, Value> ENTRY = new SimpleImmutableEntry<>(createCell(10), createValue(10));
    private static final RowColumnRangeIterator ONE_ELEMENT_ROW_COLUMN_RANGE_ITERATOR =
            new LocalRowColumnRangeIterator(ImmutableList.of(ENTRY).iterator());
    private static final Map<Cell, Value> VALUE_BY_CELL = Map.of(
            createCell(10), createValue(10),
            createCell(20), createValue(20),
            createCell(30), createValue(30));

    // this has to be an anonymous inner class rather than lambda in order to spy
    private static final ToLongFunction<Entry<Cell, Value>> ENTRY_MEASURER = new ToLongFunction<>() {
        @Override
        public long applyAsLong(Entry<Cell, Value> value) {
            return 1L;
        }
    };

    @Test
    public void oneElementTrackingRowColumnRangeIteratorIsWiredCorrectly() {
        Consumer<Long> tracker = spy(TrackingIteratorTestUtils.noOp());
        ToLongFunction<Entry<Cell, Value>> measurer = spy(ENTRY_MEASURER);
        TrackingRowColumnRangeIterator trackingIterator =
                new TrackingRowColumnRangeIterator(ONE_ELEMENT_ROW_COLUMN_RANGE_ITERATOR, tracker, measurer);

        assertThat(trackingIterator).toIterable().containsExactlyElementsOf(List.of(ENTRY));
        verify(measurer, times(1)).applyAsLong(ENTRY);
        verify(tracker, times(1)).accept(ENTRY_MEASURER.applyAsLong(ENTRY));
        verifyNoMoreInteractions(tracker);
        verifyNoMoreInteractions(measurer);
    }

    @Test
    public void multiElementTrackingRowColumnRangeIteratorIsWiredCorrectly() {
        ArrayList<Long> consumed = new ArrayList<>();
        TrackingRowColumnRangeIterator trackingIterator =
                new TrackingRowColumnRangeIterator(createRowColumnRangeIterator(), consumed::add, ENTRY_MEASURER);

        assertThat(trackingIterator)
                .toIterable()
                .containsExactlyElementsOf(ImmutableList.copyOf(createRowColumnRangeIterator()));

        assertThat(consumed)
                .containsExactlyElementsOf(StreamEx.of(createRowColumnRangeIterator())
                        .mapToLong(ENTRY_MEASURER)
                        .boxed()
                        .toList());
    }

    private static RowColumnRangeIterator createRowColumnRangeIterator() {
        return new LocalRowColumnRangeIterator(VALUE_BY_CELL.entrySet().iterator());
    }

    private static Cell createCell(int size) {
        Preconditions.checkArgument(size >= 2, "size should be at least 2");
        return Cell.create(new byte[size / 2], new byte[size - (size / 2)]);
    }

    private static Value createValue(int size) {
        Preconditions.checkArgument(size >= Long.BYTES, "size should be at least the number of bytes in one long");
        return Value.create(new byte[size - Long.BYTES], Value.INVALID_VALUE_TIMESTAMP);
    }
}
