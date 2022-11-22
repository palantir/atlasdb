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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.palantir.atlasdb.keyvalue.api.CandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ImmutableCandidateCellForSweeping;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;
import com.palantir.atlasdb.keyvalue.api.RowColumnRangeIterator;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.LocalRowColumnRangeIterator;
import com.palantir.logsafe.Preconditions;
import com.palantir.util.paging.SimpleTokenBackedResultsPage;
import com.palantir.util.paging.TokenBackedBasicResultsPage;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.commons.lang3.StringUtils;

/**
 * Utilities to create example objects and collection with a given size according to
 * {@link com.palantir.atlasdb.util.Measurable} and {@link com.palantir.atlasdb.util.MeasuringUtils}.
 * For methods returning collections, examples have 3 elements as that is the simplest non-trivial count to implement.
 */
final class TrackingKeyValueServiceTestUtils {
    static ImmutableMultimap<Cell, Long> createLongByCellMultimapWithSize(int size) {
        int valuesSize = size - 3 * Long.BYTES;
        return ImmutableSetMultimap.of(
                createCellWithSize(valuesSize / 3, (byte) 0), 0L,
                createCellWithSize(valuesSize / 3, (byte) 0), 1L,
                createCellWithSize(valuesSize - 2 * (valuesSize / 3), (byte) 2), 2L);
    }

    static ImmutableMap<RangeRequest, TokenBackedBasicResultsPage<RowResult<Value>, byte[]>>
            createPageByRangeRequestMapWithSize(int size) {
        return ImmutableMap.of(
                createRangeRequest((byte) 1), createTokenBackedBasicResultsPageWithSize(size / 3, (byte) 1),
                createRangeRequest((byte) 2), createTokenBackedBasicResultsPageWithSize(size / 3, (byte) 2),
                createRangeRequest((byte) 3),
                        createTokenBackedBasicResultsPageWithSize(size - 2 * (size / 3), (byte) 3));
    }

    static ImmutableList<byte[]> createListOfByteArraysWithSize(int size) {
        return ImmutableList.of(
                createBytesWithSize(size / 3, (byte) 1),
                createBytesWithSize(size / 3, (byte) 2),
                createBytesWithSize(size - 2 * (size / 3), (byte) 3));
    }

    static ImmutableMap<TableReference, byte[]> createByteArrayByTableReferenceMapWithSize(int size) {
        return ImmutableMap.of(
                createTableReferenceWithSize(size / 6, 'A'), createBytesWithSize(size / 6, (byte) 0),
                createTableReferenceWithSize(size / 6, 'B'), createBytesWithSize(size / 6, (byte) 1),
                createTableReferenceWithSize(size / 6, 'C'), createBytesWithSize(size - 5 * (size / 6), (byte) 2));
    }

    static ImmutableSet<TableReference> createTableReferenceSetWithSize(int size) {
        return ImmutableSet.of(
                createTableReferenceWithSize(size / 3, 'A'),
                createTableReferenceWithSize(size / 3, 'B'),
                createTableReferenceWithSize(size - 2 * (size / 3), 'C'));
    }

    static ImmutableList<RowResult<Value>> createValueRowResultListWithSize(int size) {
        return ImmutableList.of(
                createValueRowResultWithSize(size / 3, (byte) 0),
                createValueRowResultWithSize(size / 3, (byte) 1),
                createValueRowResultWithSize(size - 2 * (size / 3), (byte) 2));
    }

    static ImmutableList<RowResult<Set<Long>>> createLongSetRowResultListWithSize(int size) {
        return ImmutableList.of(
                createLongSetRowResultWithSize(size / 3, (byte) 0),
                createLongSetRowResultWithSize(size / 3, (byte) 1),
                createLongSetRowResultWithSize(size - 2 * (size / 3), (byte) 2));
    }

    static ImmutableList<ImmutableList<CandidateCellForSweeping>> createCandidateCellForSweepingTableWithSize(
            int size) {
        return ImmutableList.of(
                createCandidateCellForSweepingList(size / 3, (byte) 0),
                createCandidateCellForSweepingList(size / 3, (byte) 1),
                createCandidateCellForSweepingList(size - 2 * (size / 3), (byte) 2));
    }

    static ImmutableMap<Cell, Value> createValueByCellMapWithSize(int size) {
        return ImmutableMap.of(
                createCellWithSize(size / 6, (byte) 0), createValueWithSize(size / 6),
                createCellWithSize(size / 6, (byte) 1), createValueWithSize(size / 6),
                createCellWithSize(size / 6, (byte) 2), createValueWithSize(size - 5 * (size / 6)));
    }

    static ImmutableMap<Cell, Long> createLongByCellMapWithSize(int size) {
        int keySetSize = size - 3 * Long.BYTES;
        return ImmutableMap.of(
                createCellWithSize(keySetSize / 3, (byte) 1), 1L,
                createCellWithSize(keySetSize / 3, (byte) 2), 2L,
                createCellWithSize(keySetSize - 2 * (keySetSize / 3), (byte) 3), 3L);
    }

    static Map<byte[], RowColumnRangeIterator> createRowColumnRangeIteratorByByteArrayMutableMapWithSize(int size) {
        IdentityHashMap<byte[], RowColumnRangeIterator> mutableMap = new IdentityHashMap<>();
        mutableMap.put(
                createBytesWithSize(size / 6, (byte) 0),
                new LocalRowColumnRangeIterator(
                        createValueByCellMapWithSize(size / 6).entrySet().iterator()));
        mutableMap.put(
                createBytesWithSize(size / 6, (byte) 1),
                new LocalRowColumnRangeIterator(
                        createValueByCellMapWithSize(size / 6).entrySet().iterator()));
        mutableMap.put(
                createBytesWithSize(size / 6, (byte) 2),
                new LocalRowColumnRangeIterator(createValueByCellMapWithSize(size - 5 * (size / 6))
                        .entrySet()
                        .iterator()));
        return mutableMap;
    }

    private static RangeRequest createRangeRequest(byte identifier) {
        return RangeRequest.builder()
                .startRowInclusive(createBytesWithSize(10, identifier))
                .build();
    }

    private static ImmutableList<CandidateCellForSweeping> createCandidateCellForSweepingList(
            int size, byte identifier) {
        return ImmutableList.of(
                createCandidateCellForSweepingWithSize(size / 3, identifier),
                createCandidateCellForSweepingWithSize(size / 3, identifier),
                createCandidateCellForSweepingWithSize(size - 2 * (size / 3), identifier));
    }

    private static CandidateCellForSweeping createCandidateCellForSweepingWithSize(int size, byte identifier) {
        return ImmutableCandidateCellForSweeping.builder()
                .cell(createCellWithSize(size, identifier))
                .sortedTimestamps(List.of())
                .isLatestValueEmpty(true)
                .build();
    }

    private static RowResult<Set<Long>> createLongSetRowResultWithSize(int size, byte identifier) {
        int cellSize = 8 + (size % 8);
        int setSize = (size - cellSize) / 8;
        return RowResult.of(
                createCellWithSize(cellSize, identifier),
                LongStream.range(0, setSize).boxed().collect(Collectors.toUnmodifiableSet()));
    }

    private static RowResult<Value> createValueRowResultWithSize(int size, byte identifier) {
        return RowResult.of(createCellWithSize(size / 2, identifier), createValueWithSize(size - (size / 2)));
    }

    private static Cell createCellWithSize(int size, byte element) {
        Preconditions.checkArgument(size >= 2, "Size should be at least 2");
        return Cell.create(createBytesWithSize(size / 2, element), createBytesWithSize(size - (size / 2), element));
    }

    private static TableReference createTableReferenceWithSize(int size, char identifier) {
        Preconditions.checkArgument(size >= Character.BYTES, "size needs to be at least Character.BYTES");
        Preconditions.checkArgument(size % Character.BYTES == 0, "size needs to be divisible by Character.BYTES");
        return TableReference.createWithEmptyNamespace(StringUtils.repeat(identifier, (size / Character.BYTES) - 1));
    }

    private static TokenBackedBasicResultsPage<RowResult<Value>, byte[]> createTokenBackedBasicResultsPageWithSize(
            int size, byte identifier) {
        return new SimpleTokenBackedResultsPage<>(
                createBytesWithSize(10, identifier), createValueRowResultListWithSize(size, identifier));
    }

    private static ImmutableList<RowResult<Value>> createValueRowResultListWithSize(int size, byte identifier) {
        return ImmutableList.of(
                createValueRowResultWithSize(size / 3, identifier),
                createValueRowResultWithSize(size / 3, identifier),
                createValueRowResultWithSize(size - 2 * (size / 3), identifier));
    }

    private static Value createValueWithSize(int size) {
        Preconditions.checkArgument(size >= Long.BYTES, "Size should be at least the size in bytes of one long");
        return Value.create(createBytesWithSize(size - Long.BYTES, (byte) 0), Value.INVALID_VALUE_TIMESTAMP);
    }

    private static byte[] createBytesWithSize(int size, byte identifier) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, identifier);
        return bytes;
    }
}
