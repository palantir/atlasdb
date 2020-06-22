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
import java.util.Map;

final class ProfilingEventHandler<T, R> implements EventHandler<BatchElement<T, R>> {

    private final EventHandler<BatchElement<T, R>> delegateHandler;
    private final BatchSizeRecorder batchSizeRecorder;

    private int elementsSeenSoFar;

    ProfilingEventHandler(
            EventHandler<BatchElement<T, R>> delegateHandler,
            String safeIdentifier,
            Map<String, String> tags) {
        this.delegateHandler = delegateHandler;
        this.batchSizeRecorder = BatchSizeRecorder.create(safeIdentifier, tags);
    }

    @Override
    public void onEvent(BatchElement<T, R> event, long sequence, boolean endOfBatch) throws Exception {
        elementsSeenSoFar++;
        delegateHandler.onEvent(event, sequence, endOfBatch);

        if (endOfBatch) {
            // Shouldn't affect clients, because futures have already been completed
            batchSizeRecorder.markBatchProcessed(elementsSeenSoFar);
            elementsSeenSoFar = 0;
        }
    }
}
