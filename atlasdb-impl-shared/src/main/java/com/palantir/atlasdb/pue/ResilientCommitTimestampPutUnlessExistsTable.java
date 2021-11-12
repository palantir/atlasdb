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
import com.palantir.atlasdb.transaction.encoding.ToDoEncodingStrategy;
import com.palantir.common.streams.KeyedStream;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ResilientCommitTimestampPutUnlessExistsTable implements PutUnlessExistsTable<Long, Long> {
    private final ConsensusForgettingStore store;
    private final ToDoEncodingStrategy encodingStrategy;

    public ResilientCommitTimestampPutUnlessExistsTable(
            ConsensusForgettingStore store, ToDoEncodingStrategy encodingStrategy) {
        this.store = store;
        this.encodingStrategy = encodingStrategy;
    }

    @Override
    public void putUnlessExists(Long startTimestamp, Long commitTimestamp) throws KeyAlreadyExistsException {
        Cell cell = encodingStrategy.encodeStartTimestampAsCell(startTimestamp);
        byte[] stagingValue = pueStaging(cell, startTimestamp, commitTimestamp);
        putCommitted(cell, stagingValue);
    }

    @Override
    public void putUnlessExistsMultiple(Map<Long, Long> keyValues) throws KeyAlreadyExistsException {
        Map<Cell, Long> cellToStartTs = keyValues.keySet().stream()
                .collect(Collectors.toMap(encodingStrategy::encodeStartTimestampAsCell, x -> x));
        Map<Cell, byte[]> stagingValues = KeyedStream.stream(cellToStartTs)
                .map(startTs -> encodingStrategy.encodeCommitTimestampAsValue(
                        startTs, PutUnlessExistsValue.staging(keyValues.get(startTs))))
                .collectToMap();
        store.putUnlessExists(stagingValues);
        store.put(KeyedStream.stream(stagingValues)
                .map(encodingStrategy::transformStagingToCommitted)
                .collectToMap());
    }

    @Override
    public ListenableFuture<Long> get(Long startTs) {
        Cell cell = encodingStrategy.encodeStartTimestampAsCell(startTs);
        ListenableFuture<Optional<byte[]>> asyncRead = store.get(cell);
        return Futures.transform(asyncRead, read -> processRead(cell, startTs, read), MoreExecutors.directExecutor());
    }

    @Override
    public ListenableFuture<Map<Long, Long>> get(Iterable<Long> cells) {
        Map<Long, Cell> startTsToCell = StreamSupport.stream(cells.spliterator(), false)
                .collect(Collectors.toMap(x -> x, encodingStrategy::encodeStartTimestampAsCell));
        ListenableFuture<Map<Cell, byte[]>> asyncReads = store.getMultiple(startTsToCell.values());
        // todo(gmaretic): This can be further optimised if we add a multi check and touch, also can use multiput
        return Futures.transform(
                asyncReads,
                presentValues -> KeyedStream.stream(startTsToCell)
                        .map((startTs, cell) ->
                                processRead(cell, startTs, Optional.ofNullable(presentValues.get(cell))))
                        .collectToMap(),
                MoreExecutors.directExecutor());
    }

    private byte[] pueStaging(Cell cell, long startTs, long commitTs) {
        byte[] stagingValue =
                encodingStrategy.encodeCommitTimestampAsValue(startTs, PutUnlessExistsValue.staging(commitTs));
        store.putUnlessExists(cell, stagingValue);
        return stagingValue;
    }

    private void putCommitted(Cell cell, byte[] stagingValue) {
        store.put(cell, encodingStrategy.transformStagingToCommitted(stagingValue));
    }

    private Long processRead(Cell cell, Long startTs, Optional<byte[]> maybeActual) {
        if (maybeActual.isEmpty()) {
            return null;
        }

        byte[] actual = maybeActual.get();
        PutUnlessExistsValue<Long> valueSeen = encodingStrategy.decodeValueAsCommitTimestamp(startTs, actual);

        Long commitTs = valueSeen.value();
        if (valueSeen.isCommitted()) {
            return commitTs;
        }
        store.checkAndTouch(cell, actual);
        putCommitted(cell, actual);
        return commitTs;
    }
}
