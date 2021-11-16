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
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Value;

@SuppressWarnings("all")
@NotThreadSafe
public class CassandraImitatingConsensusForgettingStore implements ConsensusForgettingStore {
    private final double probabilityOfFailure;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final int quorum = 3;
    private final AtomicLong timestamps = new AtomicLong(0);
    private final List<Map<Cell, BytesAndTimestamp>> nodes = IntStream.range(0, 5)
            .mapToObj(_ignore -> new HashMap<Cell, BytesAndTimestamp>())
            .collect(Collectors.toList());

    public CassandraImitatingConsensusForgettingStore(double probabilityOfFailure) {
        this.probabilityOfFailure = probabilityOfFailure;
    }

    @Override
    public void putUnlessExists(Cell cell, byte[] value) throws KeyAlreadyExistsException {
        shuffleNodes();
        for (int i = 0; i < quorum; i++) {
            if (nodes.get(i).get(cell) != null) {
                throw new KeyAlreadyExistsException("nice try");
            }
        }

        writeToQuorum(cell, value);
    }

    @Override
    public void putUnlessExists(Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        values.forEach(this::putUnlessExists);
    }

    @Override
    public void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException {
        shuffleNodes();
        ListenableFuture<Optional<byte[]>> read = get(cell);
        if (!Arrays.equals(AtlasFutures.getDone(read).get(), value)) {
            throw new CheckAndSetException("not happening");
        }

        writeToQuorum(cell, value);
    }

    @Override
    public ListenableFuture<Optional<byte[]>> get(Cell cell) {
        shuffleNodes();
        Optional<BytesAndTimestamp> latest = IntStream.range(0, quorum)
                .mapToObj(nodes::get)
                .map(node -> node.get(cell))
                .filter(Objects::nonNull)
                .max(Comparator.comparing(BytesAndTimestamp::timestamp));
        if (latest.isEmpty()) {
            return Futures.immediateFuture(Optional.empty());
        }
        BytesAndTimestamp latestPresent = latest.get();
        for (int i = 0; i < quorum; i++) {
            nodes.get(i).put(cell, latestPresent);
        }
        return Futures.immediateFuture(latest.map(BytesAndTimestamp::bytes));
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getMultiple(Iterable<Cell> cells) {
        // too much effort to implement general case since it won't be used
        Cell onlyCell = Iterables.getOnlyElement(cells);
        return Futures.transform(
                get(onlyCell),
                maybeValue -> maybeValue
                        .map(value -> ImmutableMap.of(onlyCell, value))
                        .orElseGet(ImmutableMap::of),
                MoreExecutors.directExecutor());
    }

    @Override
    public void put(Cell cell, byte[] value) {
        shuffleNodes();
        for (int i = 0; i < quorum; i++) {
            nodes.get(i).put(cell, ImmutableBytesAndTimestamp.of(value, KvsConsensusForgettingStore.PUT_TIMESTAMP));
            maybeFail();
        }
    }

    @Override
    public void put(Map<Cell, byte[]> values) {
        values.forEach(this::put);
    }

    private void shuffleNodes() {
        Collections.shuffle(nodes);
    }

    private void maybeFail() {
        if (random.nextDouble() <= probabilityOfFailure) {
            throw new SafeRuntimeException("Ohno!");
        }
    }

    private void writeToQuorum(Cell cell, byte[] value) {
        maybeFail();
        long ts = timestamps.getAndIncrement();
        for (int i = 0; i < quorum; i++) {
            nodes.get(i).put(cell, ImmutableBytesAndTimestamp.of(value, ts));
            maybeFail();
        }
    }

    @Value.Immutable
    interface BytesAndTimestamp extends Comparable<BytesAndTimestamp> {
        @Value.Parameter
        byte[] bytes();

        @Value.Parameter
        long timestamp();

        @Override
        default int compareTo(BytesAndTimestamp other) {
            if (other == null) {
                return 1;
            }
            return Comparator.comparing(BytesAndTimestamp::timestamp).compare(this, other);
        }
    }
}
