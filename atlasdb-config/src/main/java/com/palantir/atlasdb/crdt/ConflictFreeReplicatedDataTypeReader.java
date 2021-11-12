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

import com.google.common.hash.Hashing;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.common.streams.KeyedStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConflictFreeReplicatedDataTypeReader<T> {
    private final KeyValueService keyValueService;
    private final TableReference crdtTable;
    private final Function<byte[], >

    public ConflictFreeReplicatedDataTypeReader(
            KeyValueService keyValueService,
            TableReference crdtTable) {
        this.keyValueService = keyValueService;
        this.crdtTable = crdtTable;
    }

    public Map<Series, T> read(List<Series> seriesList) {
        Map<Cell, Value> values = keyValueService.getRows(crdtTable,
                seriesList.stream().map(ConflictFreeReplicatedDataTypeReader::serializeSeries).collect(Collectors.toList()),
                ColumnSelection.all(),
                Long.MAX_VALUE);

    }

    @SuppressWarnings("all") // murmur3_128
    private static byte[] serializeSeries(Series series) {
        byte[] seriesBytes = ValueType.STRING.convertFromJava(series.value());
        return EncodingUtils.add(Hashing.murmur3_128().hashBytes(seriesBytes).asBytes(), seriesBytes);
    }
}
