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

package com.palantir.atlasdb.atomic;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.transaction.encoding.TimestampEncodingStrategy;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SimpleCommitTimestampAtomicTable implements AtomicTable<Long, TransactionStatus> {

    private final KeyValueService kvs;
    private final TableReference tableRef;
    private final TimestampEncodingStrategy<TransactionStatus> encodingStrategy;

    public SimpleCommitTimestampAtomicTable(
            KeyValueService kvs,
            TableReference tableRef,
            TimestampEncodingStrategy<TransactionStatus> encodingStrategy) {
        this.kvs = kvs;
        this.tableRef = tableRef;
        this.encodingStrategy = encodingStrategy;
    }

    @Override
    public void markInProgress(Iterable<Long> keys) {
        // no op
    }

    @Override
    public void updateMultiple(Map<Long, TransactionStatus> values) throws KeyAlreadyExistsException {
        kvs.putUnlessExists(
                tableRef,
                KeyedStream.stream(values)
                        .mapEntries((startTs, commitTs) -> Map.entry(
                                encodingStrategy.encodeStartTimestampAsCell(startTs),
                                encodingStrategy.encodeCommitStatusAsValue(startTs, commitTs)))
                        .collectToMap());
    }

    @Override
    public ListenableFuture<Map<Long, TransactionStatus>> get(Iterable<Long> cells) {
        Map<Long, Cell> startTsToCell = StreamSupport.stream(cells.spliterator(), false)
                .collect(Collectors.toMap(x -> x, encodingStrategy::encodeStartTimestampAsCell));

        ListenableFuture<Map<Cell, Value>> result = kvs.getAsync(
                tableRef, startTsToCell.values().stream().collect(Collectors.toMap(x -> x, _ignore -> Long.MAX_VALUE)));
        return Futures.transform(
                result,
                presentValues -> KeyedStream.stream(startTsToCell)
                        .map(presentValues::get)
                        .map(value -> value == null ? null : value.getContents())
                        .map(encodingStrategy::decodeValueAsCommitStatus)
                        .collectToMap(),
                MoreExecutors.directExecutor());
    }
}
