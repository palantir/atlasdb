/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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
import java.util.List;

final class TracingEventHandler<I, O> implements EventHandler<BatchElement<I, O>> {
    private final EventHandler<BatchElement<I, O>> handler;
    private final List<BatchElement<I, O>> pending;

    TracingEventHandler(EventHandler<BatchElement<I, O>> delegate, int bufferSize) {
        this.handler = delegate;
        this.pending = new ArrayList<>(bufferSize);
    }

    @Override
    public void onEvent(BatchElement<I, O> event, long sequence, boolean endOfBatch) throws Exception {
        pending.add(event);
        if (endOfBatch) {
            pending.forEach(e -> e.result().running());
            handler.onEvent(event, sequence, true);
            pending.clear();
        } else {
            handler.onEvent(event, sequence, false);
        }
    }
}
