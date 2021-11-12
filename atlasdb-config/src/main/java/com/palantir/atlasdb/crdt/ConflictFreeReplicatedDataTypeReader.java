/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.crdt;

import com.google.common.collect.Streams;
import com.palantir.atlasdb.crdt.generated.CrdtTable;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.BatchColumnRangeSelection;
import com.palantir.common.streams.KeyedStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConflictFreeReplicatedDataTypeReader<T> {
    private final CrdtTable crdtTable;
    private final ConflictFreeReplicatedDataTypeAdapter<T> adapter;

    public ConflictFreeReplicatedDataTypeReader(CrdtTable crdtTable, ConflictFreeReplicatedDataTypeAdapter<T> adapter) {
        this.crdtTable = crdtTable;
        this.adapter = adapter;
    }

    public Map<Series, T> read(List<Series> seriesList) {
        Map<CrdtTable.CrdtRow, Iterator<CrdtTable.CrdtColumnValue>> values = crdtTable.getRowsColumnRangeIterator(
                seriesList.stream()
                        .map(series -> CrdtTable.CrdtRow.of(series.value()))
                        .collect(Collectors.toList()),
                BatchColumnRangeSelection.create(PtBytes.EMPTY_BYTE_ARRAY, PtBytes.EMPTY_BYTE_ARRAY, 1_000));

        return KeyedStream.stream(values)
                .<Series>mapKeys(row -> ImmutableSeries.of(row.getSeries()))
                .map(this::processValueIterator)
                .collectToMap();
    }

    private T processValueIterator(Iterator<CrdtTable.CrdtColumnValue> valueIterator) {
        return Streams.stream(valueIterator)
                .map(cv -> adapter.deserializer().apply(cv.getValue()))
                .reduce(adapter.identity(), (first, second) -> adapter.merge().apply(first, second));
    }
}
