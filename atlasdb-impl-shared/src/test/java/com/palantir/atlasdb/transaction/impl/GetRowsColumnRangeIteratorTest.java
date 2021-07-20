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

package com.palantir.atlasdb.transaction.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GetRowsColumnRangeIteratorTest {

    private static final TableReference TABLE_REFERENCE = TableReference.createWithEmptyNamespace("test");
    private static final byte[] ROW = "row".getBytes(StandardCharsets.UTF_8);
    private static final int BATCH_SIZE = 10;
    public static final BatchColumnRangeSelection COLUMN_RANGE_SELECTION =
            BatchColumnRangeSelection.create(null, null, BATCH_SIZE);

    private final KeyValueService kvs = new InMemoryKeyValueService(true);
    private final ColumnRangeBatchProvider batchProvider =
            new ColumnRangeBatchProvider(kvs, TABLE_REFERENCE, ROW, COLUMN_RANGE_SELECTION, Long.MAX_VALUE);

    @Test
    public void ifBatchIsEmptyNoValidateCallsAreMade() {
        Runnable validationStep = mock(Runnable.class);
        Iterator<Map.Entry<Cell, byte[]>> iterator = createIteratorUnderTest(validationStep);

        List<Map.Entry<Cell, byte[]>> entries = ImmutableList.copyOf(iterator);

        assertThat(entries).isEmpty();
        verifyNoInteractions(validationStep);
    }

    @Test
    public void firstBatchHasNoValidation() {
        Runnable validationStep = mock(Runnable.class);
        Set<Cell> puts = putColumns(BATCH_SIZE + 5);

        int limit = BATCH_SIZE - 1;
        Iterator<Map.Entry<Cell, byte[]>> iteratorUnderTest = createIteratorUnderTest(validationStep);

        // still under the first batch size limit
        List<Map.Entry<Cell, byte[]>> firstBatchBarOne =
                ImmutableList.copyOf(Iterators.limit(iteratorUnderTest, limit));
        verifyNoInteractions(validationStep);

        // the last element in the first batch is still on the first batch
        Map.Entry<Cell, byte[]> lastInFirstBatch = Iterators.getNext(iteratorUnderTest, null);
        assertThat(lastInFirstBatch).isNotNull();
        verifyNoInteractions(validationStep);

        // consume one more i.e. batch size amount
        Map.Entry<Cell, byte[]> firstInSecondBatch = Iterators.getNext(iteratorUnderTest, null);
        assertThat(firstInSecondBatch).isNotNull();
        verify(validationStep, times(1)).run();

        // validation step is called per batch
        ImmutableList<Map.Entry<Cell, byte[]>> restOfSecondBatch = ImmutableList.copyOf(iteratorUnderTest);
        verifyNoMoreInteractions(validationStep);

        List<Cell> cellsReadInOrder = Streams.stream(Iterables.concat(
                        firstBatchBarOne, ImmutableList.of(lastInFirstBatch, firstInSecondBatch), restOfSecondBatch))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        assertThat(cellsReadInOrder)
                .as("we can read all that we wrote")
                .hasSameElementsAs(puts)
                .as("iterator returns cells back in sorted order")
                .isSorted();
    }

    @Test
    public void validationCalledNumberOfBatchesMinusOneTimes() {
        Runnable validationStep = mock(Runnable.class);
        putColumns(13 * BATCH_SIZE + 8);

        Iterator<Map.Entry<Cell, byte[]>> iteratorUnderTest = createIteratorUnderTest(validationStep);
        List<Map.Entry<Cell, byte[]>> consumedEntries = ImmutableList.copyOf(iteratorUnderTest);

        assertThat(consumedEntries).hasSize(13 * BATCH_SIZE + 8);

        verify(validationStep, times(14 - 1)).run();
    }

    private Set<Cell> putColumns(int numberOfColumns) {
        byte[] value = new byte[1];
        Map<Cell, byte[]> puts = IntStream.range(0, numberOfColumns)
                .mapToObj(i -> String.format("cell%02d", i).getBytes(StandardCharsets.UTF_8))
                .map(column -> Cell.create(ROW, column))
                .collect(ImmutableMap.toImmutableMap(Function.identity(), _unused -> value));
        kvs.put(TABLE_REFERENCE, puts, 1L);

        return puts.keySet();
    }

    private RowColumnRangeIterator getInitialIterator() {
        return kvs.getRowsColumnRange(TABLE_REFERENCE, ImmutableList.of(ROW), COLUMN_RANGE_SELECTION, Long.MAX_VALUE)
                .get(ROW);
    }

    private Iterator<Map.Entry<Cell, byte[]>> createIteratorUnderTest(Runnable validationStep) {
        return GetRowsColumnRangeIterator.iterator(
                batchProvider,
                getInitialIterator(),
                COLUMN_RANGE_SELECTION,
                validationStep,
                results -> Maps.transformValues(results, Value::getContents).entrySet());
    }
}
