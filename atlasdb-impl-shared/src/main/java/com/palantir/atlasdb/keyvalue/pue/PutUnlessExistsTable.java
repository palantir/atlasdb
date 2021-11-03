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

package com.palantir.atlasdb.keyvalue.pue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Optional;

public final class PutUnlessExistsTable<V> {
    private final InternalPutUnlessExistsTable internalTable;
    private final ValueSerializers<V> serializers;

    public PutUnlessExistsTable(InternalPutUnlessExistsTable internalTable, ValueSerializers<V> serializers) {
        this.internalTable = internalTable;
        this.serializers = serializers;
    }

    public ListenableFuture<Optional<V>> get(Cell c) {
        return Futures.transform(
                get(ImmutableSet.of(c)), result -> Optional.ofNullable(result.get(c)), MoreExecutors.directExecutor());
    }

    public ListenableFuture<Map<Cell, V>> get(Iterable<Cell> cells) {
        return Futures.transform(
                internalTable.get(cells),
                result -> KeyedStream.stream(result)
                        .map(serializers.byteDeserializer())
                        .collectToMap(),
                MoreExecutors.directExecutor());
    }

    public void putUnlessExists(Cell c, V value) throws KeyAlreadyExistsException {
        putUnlessExistsMultiple(ImmutableMap.of(c, value));
    }

    public void putUnlessExistsMultiple(Map<Cell, V> values) throws KeyAlreadyExistsException {
        internalTable.putUnlessExistsMultiple(
                KeyedStream.stream(values).map(serializers.byteSerializer()).collectToMap());
    }
}
