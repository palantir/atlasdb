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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class TimedConsensusForgettingStore implements ConsensusForgettingStore {
    private final ConsensusForgettingStore delegate;
    private final ConsensusForgettingStoreMetrics metrics;
    private final AtomicInteger concurrentCheckAndTouchOperations;

    @VisibleForTesting
    TimedConsensusForgettingStore(
            ConsensusForgettingStore delegate,
            ConsensusForgettingStoreMetrics metrics,
            AtomicInteger concurrentCheckAndTouchOperations) {
        this.delegate = delegate;
        this.metrics = metrics;
        this.concurrentCheckAndTouchOperations = concurrentCheckAndTouchOperations;
    }

    public static ConsensusForgettingStore create(ConsensusForgettingStore delegate, TaggedMetricRegistry registry) {
        ConsensusForgettingStoreMetrics metrics = ConsensusForgettingStoreMetrics.of(registry);
        AtomicInteger concurrentCheckAndTouchOperationTracker = new AtomicInteger(0);
        metrics.concurrentCheckAndTouches(concurrentCheckAndTouchOperationTracker::get);
        return new TimedConsensusForgettingStore(delegate, metrics, concurrentCheckAndTouchOperationTracker);
    }

    @Override
    public void putUnlessExists(Cell cell, byte[] value) throws KeyAlreadyExistsException {
        delegate.putUnlessExists(cell, value);
    }

    @Override
    public void putUnlessExists(Map<Cell, byte[]> values) throws KeyAlreadyExistsException {
        delegate.putUnlessExists(values);
    }

    @Override
    public void checkAndTouch(Cell cell, byte[] value) throws CheckAndSetException {
        concurrentCheckAndTouchOperations.incrementAndGet();
        try {
            metrics.checkAndTouch().time(() -> delegate.checkAndTouch(cell, value));
        } finally {
            concurrentCheckAndTouchOperations.decrementAndGet();
        }
    }

    @Override
    public ListenableFuture<Optional<byte[]>> get(Cell cell) {
        return delegate.get(cell);
    }

    @Override
    public ListenableFuture<Map<Cell, byte[]>> getMultiple(Iterable<Cell> cells) {
        return delegate.getMultiple(cells);
    }

    @Override
    public void put(Cell cell, byte[] value) {
        delegate.put(cell, value);
    }

    @Override
    public void put(Map<Cell, byte[]> values) {
        delegate.put(values);
    }
}
