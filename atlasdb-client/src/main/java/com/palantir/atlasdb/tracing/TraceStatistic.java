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

import java.util.concurrent.atomic.AtomicLong;

public final class TraceStatistic {
    private static final TraceStatistic NOT_OBSERVED_TRACE = empty();

    private final AtomicLong emptyReads;
    private final AtomicLong skippedValues;
    private final AtomicLong bytesReadFromDb;

    private TraceStatistic(long emptyReads, long skippedValues, long bytesReadFromDb) {
        this.emptyReads = new AtomicLong(emptyReads);
        this.skippedValues = new AtomicLong(skippedValues);
        this.bytesReadFromDb = new AtomicLong(bytesReadFromDb);
    }

    boolean isEmpty() {
        return bytesReadFromDb.get() == 0 && skippedValues.get() == 0 && emptyReads.get() == 0;
    }

    TraceStatistic copy() {
        return of(emptyReads.get(), skippedValues.get(), bytesReadFromDb.get());
    }

    public long emptyReads() {
        return emptyReads.get();
    }

    public long skippedValues() {
        return skippedValues.get();
    }

    public long bytesReadFromDb() {
        return bytesReadFromDb.get();
    }

    void incEmptyReads(long count) {
        emptyReads.addAndGet(count);
    }

    void incSkippedValues(long count) {
        skippedValues.addAndGet(count);
    }

    void incBytesReadFromDb(long bytes) {
        bytesReadFromDb.addAndGet(bytes);
    }

    static TraceStatistic empty() {
        return of(0L, 0L, 0L);
    }

    static TraceStatistic notObserved() {
        return NOT_OBSERVED_TRACE;
    }

    static TraceStatistic of(long emptyReads, long skippedValues, long bytesReadFromDb) {
        return new TraceStatistic(emptyReads, skippedValues, bytesReadFromDb);
    }
}
