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

/**
 * Helper to track trace/span-level statistics. This relies on {@link ExecutorInheritableThreadLocal} for the tracking
 * of values.
 */
public final class TraceStatistics {
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

    /**
     * Get the current trace statistic instance and clear it. This resets the statistics for the current thread.
     *
     * Use `getCopyAndRestoreOriginal` to restore the instance returned by this method.
     */
    public static TraceStatistic getCurrentAndClear() {
        if (!Tracer.isTraceObservable()) {
            return TraceStatistic.notObserved();
        }

        TraceStatistic current = traceStatistic.get();
        traceStatistic.remove();
        return current;
    }

    /**
     * Get a direct reference to the mutable statistic. Only use this when the original thread can't be reached when
     * the span finishes. Prefer using {@link TraceStatistics}.getCopyAndRestoreOriginal().
     */
    public static TraceStatistic getReferenceToCurrent() {
        if (!Tracer.isTraceObservable()) {
            return TraceStatistic.notObserved();
        }

        return traceStatistic.get();
    }

    /**
     * Increment the number of empty values that have been read.
     */
    public static void incEmptyValues(long emptyValues) {
        if (!Tracer.isTraceObservable()) {
            return;
        }

        traceStatistic.get().incEmptyReads(emptyValues);
    }

    /**
     * Increment the number of bytes that have been read from the underlying database.
     */
    public static void incBytesRead(long bytes) {
        if (!Tracer.isTraceObservable()) {
            return;
        }

        traceStatistic.get().incBytesReadFromDb(bytes);
    }

    /**
     * Get a copy of the current statistics and restore the original statistics. A companion to `getCurrentAndClear`.
     */
    public static TraceStatistic getCopyAndRestoreOriginal(TraceStatistic original) {
        if (!Tracer.isTraceObservable()) {
            return TraceStatistic.notObserved();
        }

        TraceStatistic current = traceStatistic.get().copy();

        if (original.isEmpty()) {
            traceStatistic.remove();
        } else {
            traceStatistic.set(original);
        }

        return current;
    }
}
