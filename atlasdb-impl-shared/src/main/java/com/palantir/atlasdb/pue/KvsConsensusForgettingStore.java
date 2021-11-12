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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetCompatibility;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class KvsConsensusForgettingStore implements ConsensusForgettingStore {
    private static final long PUT_TIMESTAMP = Long.MAX_VALUE - 10;
    private final KeyValueService kvs;
    private final TableReference tableRef;

    public KvsConsensusForgettingStore(KeyValueService kvs, TableReference tableRef) {
        Preconditions.checkState(kvs.getCheckAndSetCompatibility()
                == CheckAndSetCompatibility.SUPPORTED_DETAIL_ON_FAILURE_MAY_PARTIALLY_PERSIST);
        this.kvs = kvs;
        this.tableRef = tableRef;
    }

    @Override
    public void putUnlessExists(Cell cell, byte[] value) throws KeyAlreadyExistsException {
        kvs.putUnlessExists(tableRef, ImmutableMap.of(cell, value));
    }

    @Override
    public void putUnlessExists(Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        kvs.putUnlessExists(tableRef, values);
    }

    @Override
    public void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException {
        CheckAndSetRequest request = CheckAndSetRequest.singleCell(tableRef, cell, value, value);
        kvs.checkAndSet(request);
    }

    @Override
    public ListenableFuture<Optional<byte[]>> get(Cell cell) {
        return Futures.transform(
                kvs.getAsync(tableRef, ImmutableMap.of(cell, Long.MAX_VALUE)),
                result -> Optional.ofNullable(result.get(cell)).map(Value::getContents),
                MoreExecutors.directExecutor());
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getMultiple(Iterable<Cell> cells) {
        return Futures.transform(
                kvs.getAsync(
                        tableRef,
                        StreamSupport.stream(cells.spliterator(), false)
                                .collect(Collectors.toMap(x -> x, _ignore -> Long.MAX_VALUE))),
                result -> KeyedStream.stream(result).map(Value::getContents).collectToMap(),
                MoreExecutors.directExecutor());
    }

    @Override
    public void put(Cell cell, byte[] value) {
        put(ImmutableMap.of(cell, value));
    }

    @Override
    public void put(Map<Cell, byte[]> values) {
        // todo(gmaretic): verify this is already implemented the way we need it to be
        kvs.put(tableRef, values, PUT_TIMESTAMP);
    }
}
