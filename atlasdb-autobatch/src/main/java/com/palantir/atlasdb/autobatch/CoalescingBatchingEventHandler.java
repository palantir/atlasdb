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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.lmax.disruptor.EventHandler;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher.DisruptorFuture;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.Set;

final class CoalescingBatchingEventHandler<T, R> implements EventHandler<BatchElement<T, R>> {

    private static final SafeLogger log = SafeLoggerFactory.get(CoalescingBatchingEventHandler.class);

    private final CoalescingRequestFunction<T, R> function;

    // explicitly not using Multimap to avoid expensive com.google.common.collect.AbstractMapBasedMultimap.clear()
    // that iterates and clears each value collection.
    private final Map<T, Set<DisruptorFuture<R>>> pending;

    CoalescingBatchingEventHandler(CoalescingRequestFunction<T, R> function, int bufferSize) {
        this.function = function;
        this.pending = Maps.newHashMapWithExpectedSize(bufferSize);
    }

    @Override
    public void onEvent(BatchElement<T, R> event, long sequence, boolean endOfBatch) {
        pending.computeIfAbsent(event.argument(), _key -> Sets.newHashSetWithExpectedSize(5))
                .add(event.result());
        if (endOfBatch) {
            flush();
        }
    }

    private void flush() {
        try {
            Map<T, R> results = function.apply(pending.keySet());
            pending.forEach((argument, futures) -> futures.forEach(future -> {
                if (results.containsKey(argument)) {
                    future.set(results.get(argument));
                } else {
                    log.warn(
                            "Coalescing function has violated coalescing function postcondition",
                            SafeArg.of("functionClass", function.getClass().getCanonicalName()));
                    future.setException(new PostconditionFailedException(function.getClass()));
                }
            }));
        } catch (Throwable t) {
            pending.forEach((argument, futures) -> futures.forEach(future -> future.setException(t)));
        }
        pending.clear();
    }
}
