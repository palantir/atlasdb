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

/**
 * In general, the purpose of this class at least in its current form is to evaluate the requirement (or lack
 * thereof) of batching and/or auto-batching calls to the {@link com.palantir.atlasdb.keyvalue.api.KeyValueService}.
 * In addition to tracking the time required to perform an individual {@link #checkAndTouch(Cell, byte[])} operation,
 * we also track:
 * <ul>
 *     <li>
 *         the number of concurrent check and touch operations; a high number suggests that auto-batching would be
 *         useful, and
 *     </li>
 *     <li>
 *         the size of batches that are passed through to an underlying implementation; a high number suggests that
 *         batching (not necessarily auto-batching) would be useful.
 *     </li>
 * </ul>
 */
public class InstrumentedConsensusForgettingStore implements ConsensusForgettingStore {
    private final ConsensusForgettingStore delegate;
    private final ConsensusForgettingStoreMetrics metrics;
    private final AtomicInteger concurrentCheckAndTouchOperations;

    @VisibleForTesting
    InstrumentedConsensusForgettingStore(
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
        return new InstrumentedConsensusForgettingStore(delegate, metrics, concurrentCheckAndTouchOperationTracker);
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
        runCheckAndTouchOperation(() -> delegate.checkAndTouch(cell, value));
    }

    @Override
    public void checkAndTouch(Map<Cell, byte[]> values) throws CheckAndSetException {
        metrics.batchedCheckAndTouchSize().update(values.size());
        runCheckAndTouchOperation(() -> delegate.checkAndTouch(values));
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

    private void runCheckAndTouchOperation(Runnable checkAndTouchOperation) {
        concurrentCheckAndTouchOperations.incrementAndGet();
        try {
            metrics.checkAndTouch().time(checkAndTouchOperation);
        } finally {
            concurrentCheckAndTouchOperations.decrementAndGet();
        }
    }
}
