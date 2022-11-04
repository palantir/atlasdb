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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import io.vavr.collection.Iterator;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import org.junit.Test;

public final class TrackingRowColumnRangeIteratorTest {
    private static final Entry<Cell, Value> ENTRY = new SimpleImmutableEntry<>(
            Cell.create(new byte[1], new byte[1]),
            Value.create(PtBytes.EMPTY_BYTE_ARRAY, Value.INVALID_VALUE_TIMESTAMP));

    // this has to be an anonymous inner class rather than lambda in order to spy
    private static final ToLongFunction<Entry<Cell, Value>> ENTRY_MEASURER = new ToLongFunction<>() {
        @Override
        public long applyAsLong(Entry<Cell, Value> value) {
            return 1L;
        }
    };

    @Test
    public void trackingClosableStringIteratorIsWiredCorrectly() {
        Consumer<Long> tracker = spy(TrackingIteratorTestUtils.noOp());
        ToLongFunction<Entry<Cell, Value>> measurer = spy(ENTRY_MEASURER);

        TrackingRowColumnRangeIterator trackingIterator =
                new TrackingRowColumnRangeIterator(createOneElementRowColumnRangeIterator(), tracker, measurer);

        assertThat(trackingIterator).toIterable().containsExactlyElementsOf(ImmutableSet.of(ENTRY));
        trackingIterator.forEachRemaining(TrackingIteratorTestUtils.noOp());

        verify(measurer, times(1)).applyAsLong(ENTRY);
        verify(tracker, times(1)).accept(ENTRY_MEASURER.applyAsLong(ENTRY));
        verifyNoMoreInteractions(tracker);
        verifyNoMoreInteractions(measurer);
    }

    private static RowColumnRangeIterator createOneElementRowColumnRangeIterator() {
        return new LocalRowColumnRangeIterator(Iterator.of(new SimpleImmutableEntry<>(ENTRY)));
    }
}
