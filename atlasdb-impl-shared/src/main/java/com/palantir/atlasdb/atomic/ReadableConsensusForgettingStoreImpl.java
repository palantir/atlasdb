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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ReadableConsensusForgettingStoreImpl implements ReadableConsensusForgettingStore {
    private final KeyValueService kvs;
    private final TableReference tableRef;

    public ReadableConsensusForgettingStoreImpl(KeyValueService kvs, TableReference tableRef) {
        this.kvs = kvs;
        this.tableRef = tableRef;
    }

    @Override
    public ListenableFuture<Optional<byte[]>> get(Cell cell) {
        return Futures.transform(
                getMultiple(ImmutableList.of(cell)),
                result -> Optional.ofNullable(result.get(cell)),
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
}
