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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import java.util.Map;
import java.util.Optional;

public class PueConsensusForgettingStore implements ConsensusForgettingStore {
    private final KeyValueService kvs;
    private final TableReference tableRef;
    private final ReadableConsensusForgettingStore reader;

    public PueConsensusForgettingStore(KeyValueService kvs, TableReference tableRef) {
        Preconditions.checkArgument(!kvs.getCheckAndSetCompatibility().consistentOnFailure());
        this.kvs = kvs;
        this.tableRef = tableRef;
        this.reader = new ReadableConsensusForgettingStoreImpl(kvs, tableRef);
    }

    @Override
    public AtomicUpdateResult atomicUpdate(Cell cell, byte[] value) {
        return atomicUpdate(ImmutableMap.of(cell, value)).get(cell);
    }

    @Override
    public Map<Cell, AtomicUpdateResult> atomicUpdate(Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        try {
            kvs.putUnlessExists(tableRef, values);
        } catch (KeyAlreadyExistsException ex) {
            return KeyedStream.stream(values)
                    .map(_u -> AtomicUpdateResult.failure(ex))
                    .collectToMap();
        }
        return KeyedStream.stream(values)
                .map(_u -> AtomicUpdateResult.success())
                .collectToMap();
    }

    @Override
    public void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException {
        CheckAndSetRequest request = CheckAndSetRequest.singleCell(tableRef, cell, value, value);
        kvs.checkAndSet(request);
    }

    /**
     * Note that changing this method may invalidate existing tests in
     * ResilientCommitTimestampPutUnlessExistsTableTest.
     */
    @Override
    public void checkAndTouch(Map<Cell, byte[]> values) throws CheckAndSetException {
        values.forEach(this::checkAndTouch);
    }

    @Override
    public ListenableFuture<Optional<byte[]>> get(Cell cell) {
        return reader.get(cell);
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getMultiple(Iterable<Cell> cells) {
        return reader.getMultiple(cells);
    }

    @Override
    public void put(Cell cell, byte[] value) {
        put(ImmutableMap.of(cell, value));
    }

    @Override
    public void put(Map<Cell, byte[]> values) {
        kvs.setOnce(tableRef, values);
    }
}
