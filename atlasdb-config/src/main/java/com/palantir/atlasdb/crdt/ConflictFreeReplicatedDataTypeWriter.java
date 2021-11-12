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

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.crdt.bucket.SeriesBucketSelector;
import com.palantir.atlasdb.crdt.generated.CrdtTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConflictFreeReplicatedDataTypeWriter<T> implements AutoCloseable {
    private final CrdtTable crdtTable;
    private final ConflictFreeReplicatedDataTypeAdapter<T> adapter;
    private final SeriesBucketSelector seriesBucketSelector;
    private final Map<Series, Long> acquiredPartitions = new HashMap<>();

    public ConflictFreeReplicatedDataTypeWriter(
            CrdtTable crdtTable,
            ConflictFreeReplicatedDataTypeAdapter<T> adapter,
            SeriesBucketSelector seriesBucketSelector) {
        this.crdtTable = crdtTable;
        this.adapter = adapter;
        this.seriesBucketSelector = seriesBucketSelector;
    }

    public void aggregateValue(Series series, T value) {
        long partition = acquiredPartitions.computeIfAbsent(series, seriesBucketSelector::getBucket);
        CrdtTable.CrdtRow seriesRow = CrdtTable.CrdtRow.of(series.value());
        List<CrdtTable.CrdtColumnValue> rowColumns =
                crdtTable.getRowColumns(seriesRow, CrdtTable.getColumnSelection(CrdtTable.CrdtColumn.of(partition)));

        if (rowColumns.isEmpty()) {
            crdtTable.put(
                    seriesRow,
                    CrdtTable.CrdtColumnValue.of(
                            CrdtTable.CrdtColumn.of(partition),
                            adapter.serializer().apply(value)));
            return;
        }

        CrdtTable.CrdtColumnValue presentColumnValue = Iterables.getOnlyElement(rowColumns);
        crdtTable.put(
                seriesRow,
                CrdtTable.CrdtColumnValue.of(
                        CrdtTable.CrdtColumn.of(partition),
                        adapter.serializer()
                                .apply(adapter.merge()
                                        .apply(adapter.deserializer().apply(presentColumnValue.getValue()), value))));
    }

    @Override
    public void close() {
        acquiredPartitions.forEach(seriesBucketSelector::releaseBucket);
    }
}
