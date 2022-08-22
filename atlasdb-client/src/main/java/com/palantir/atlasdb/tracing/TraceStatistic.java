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

import java.util.concurrent.atomic.LongAdder;

public final class TraceStatistic {
    private static final TraceStatistic NOT_OBSERVED_TRACE = empty();

    private final LongAdder emptyReads;
    private final LongAdder skippedValues;
    private final LongAdder bytesReadFromDb;

    private TraceStatistic(long emptyReads, long skippedValues, long bytesReadFromDb) {
        this.emptyReads = new LongAdder();
        if (emptyReads > 0) {
            this.emptyReads.add(emptyReads);
        }
        this.skippedValues = new LongAdder();
        if (skippedValues > 0) {
            this.skippedValues.add(skippedValues);
        }
        this.bytesReadFromDb = new LongAdder();
        if (bytesReadFromDb > 0) {
            this.bytesReadFromDb.add(bytesReadFromDb);
        }
    }

    boolean isEmpty() {
        return bytesReadFromDb.sum() == 0 && skippedValues.sum() == 0 && emptyReads.sum() == 0;
    }

    TraceStatistic copy() {
        return of(emptyReads.sum(), skippedValues.sum(), bytesReadFromDb.sum());
    }

    /**
     * Number of reads where the latest value is empty. This happens whenever the (row, col, ts) with the latest ts has
     * been deleted but not swept yet.
     */
    public long emptyReads() {
        return emptyReads.sum();
    }

    /**
     * Number of non-latest values read. This happens whenever range scanning and encountering multiple (row, col, ts)
     * entries are returned for the same (row, col) tuple. All but the one with the latest timestamp will be skipped.
     * This will eventually be swept.
     */
    public long skippedValues() {
        return skippedValues.sum();
    }

    /**
     * Total number of useful bytes read. This doesn't account for e.g. protocol overheads, and just tracks the sizes
     * of rows/cols/values.
     */
    public long bytesReadFromDb() {
        return bytesReadFromDb.sum();
    }

    void incEmptyReads(long count) {
        emptyReads.add(count);
    }

    void incSkippedValues(long count) {
        skippedValues.add(count);
    }

    void incBytesReadFromDb(long bytes) {
        bytesReadFromDb.add(bytes);
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
