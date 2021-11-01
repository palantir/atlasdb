/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.autobatch;

import com.lmax.disruptor.EventHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

final class IndependentBatchingEventHandler<T, R> implements EventHandler<BatchElement<T, R>> {
    private final Consumer<List<BatchElement<T, R>>> batchFunction;
    private final List<BatchElement<T, R>> pending;

    IndependentBatchingEventHandler(Consumer<List<BatchElement<T, R>>> batchFunction, int bufferSize) {
        this.batchFunction = batchFunction;
        this.pending = new ArrayList<>(bufferSize);
    }

    @Override
    public void onEvent(BatchElement<T, R> event, long _sequence, boolean endOfBatch) {
        pending.add(event);
        if (endOfBatch) {
            flush();
        }
    }

    private void flush() {
        try {
            batchFunction.accept(Collections.unmodifiableList(pending));
        } catch (Throwable t) {
            pending.forEach(p -> p.result().setException(t));
        }
        pending.clear();
    }
}
