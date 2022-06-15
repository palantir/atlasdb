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

package com.palantir.atlasdb.tracing;

import com.palantir.common.concurrent.ExecutorInheritableThreadLocal;
import com.palantir.tracing.Tracer;

public class TraceStatistics {
    private TraceStatistics() {}

    // The `TraceStatistic` object is mutable, and should be shared between the parent thread & any child threads/work
    // that get created.
    private static final ExecutorInheritableThreadLocal<TraceStatistic> traceStatistic =
            new ExecutorInheritableThreadLocal<>() {
                @Override
                protected TraceStatistic initialValue() {
                    return TraceStatistic.empty();
                }
            };

    static TraceStatistic clearAndGetOld() {
        TraceStatistic current = traceStatistic.get();
        traceStatistic.remove();
        return current;
    }

    static TraceStatistic get() {
        return traceStatistic.get().copy();
    }

    public static void incEmptyValues(long emptyValues) {
        if (!Tracer.isTraceObservable()) {
            return;
        }

        traceStatistic.get().incEmptyReads(emptyValues);
    }

    public static void restore(TraceStatistic oldValue) {
        if (oldValue.isEmpty()) {
            traceStatistic.remove();
        } else {
            traceStatistic.set(oldValue);
        }
    }
}
