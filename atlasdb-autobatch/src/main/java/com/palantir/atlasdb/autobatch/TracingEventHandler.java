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

import java.util.concurrent.Callable;

import com.lmax.disruptor.EventHandler;
import com.palantir.tracing.Observability;
import com.palantir.tracing.Tracers;

final class TracingEventHandler<I, O> implements EventHandler<BatchElement<I, O>> {
    private final EventHandler<BatchElement<I, O>> handler;
    private final String purpose;
    private final Observability observability;

    TracingEventHandler(
            EventHandler<BatchElement<I, O>> delegate,
            String purpose,
            Observability observability) {
        this.handler = delegate;
        this.purpose = purpose;
        this.observability = observability;
    }

    @Override
    public void onEvent(BatchElement<I, O> event, long sequence, boolean endOfBatch) throws Exception {
        if (endOfBatch) {
            Tracers.wrapWithNewTrace(purpose, observability, (Callable<Void>) () -> {
                handler.onEvent(event, sequence, true);
                return null;
            }).call();
        } else {
            handler.onEvent(event, sequence, false);
        }
    }
}
