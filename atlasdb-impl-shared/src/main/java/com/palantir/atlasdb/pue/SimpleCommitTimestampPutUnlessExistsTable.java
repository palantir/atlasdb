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

package com.palantir.atlasdb.pue;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.encoding.TimestampEncodingStrategy;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SimpleCommitTimestampPutUnlessExistsTable implements PutUnlessExistsTable<Long, Long> {
    private final KeyValueService kvs;
    private final TableReference tableRef;
    private final TimestampEncodingStrategy<Long> encodingStrategy;

    public SimpleCommitTimestampPutUnlessExistsTable(
            KeyValueService kvs, TableReference tableRef, TimestampEncodingStrategy<Long> encodingStrategy) {
        this.kvs = kvs;
        this.tableRef = tableRef;
        this.encodingStrategy = encodingStrategy;
    }

    @Override
    public void putUnlessExistsMultiple(Map<Long, Long> values) throws KeyAlreadyExistsException {
        kvs.putUnlessExists(
                tableRef,
                KeyedStream.stream(values)
                        .mapEntries((startTs, commitTs) -> Map.entry(
                                encodingStrategy.encodeStartTimestampAsCell(startTs),
                                encodingStrategy.encodeCommitTimestampAsValue(startTs, commitTs)))
                        .collectToMap());
    }

    @Override
    public ListenableFuture<Map<Long, Long>> get(Iterable<Long> cells) {
        Map<Long, Cell> startTsToCell = StreamSupport.stream(cells.spliterator(), false)
                .collect(Collectors.toMap(x -> x, encodingStrategy::encodeStartTimestampAsCell));

        ListenableFuture<Map<Cell, Value>> result = kvs.getAsync(
                tableRef, startTsToCell.values().stream().collect(Collectors.toMap(x -> x, _ignore -> Long.MAX_VALUE)));
        return Futures.transform(
                result,
                presentValues -> KeyedStream.stream(startTsToCell)
                        .map(presentValues::get)
                        .filter(Objects::nonNull)
                        .map(Value::getContents)
                        .map(encodingStrategy::decodeValueAsCommitTimestamp)
                        .collectToMap(),
                MoreExecutors.directExecutor());
    }
}
