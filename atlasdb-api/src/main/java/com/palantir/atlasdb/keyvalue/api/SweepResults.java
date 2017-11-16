/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.api;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public abstract class SweepResults {

    @Value.Default
    public Optional<byte[]> getPreviousStartRow() {
        return Optional.empty();
    }

    @Value.Default
    public Optional<byte[]> getNextStartRow() {
        return Optional.empty();
    }

    /**
     * The approximate number of (cell, timestamp) pairs examined.
     */
    public abstract long getCellTsPairsExamined();

    /**
     * The number of (cell, timestamp) pairs deleted.
     */
    public abstract long getStaleValuesDeleted();

    public abstract long getSweptTimestamp();

    /**
     * Time spent sweeping this iteration in milliseconds.
     */
    public abstract long getTimeInMillis();

    /**
     * Time in milliseconds when we started sweeping this table.
     */
    public abstract long getTimeSweepStarted();

    public long getTimeElapsedSinceStartedSweeping() {
        return System.currentTimeMillis() - getTimeSweepStarted();
    }

    /**
     * Returns a new {@link SweepResults} representing cumulative results from this instance and {@code other}. Assumes
     * that {@code other} represents results from subsequent iteration of sweep (i.e., it happened after the run that
     * produced this instance).
     */
    public SweepResults accumulateWith(SweepResults other) {
        return SweepResults.builder()
                .cellTsPairsExamined(getCellTsPairsExamined() + other.getCellTsPairsExamined())
                .staleValuesDeleted(getStaleValuesDeleted() + other.getStaleValuesDeleted())
                .sweptTimestamp(Math.min(getSweptTimestamp(), other.getSweptTimestamp()))
                .nextStartRow(other.getNextStartRow())
                .timeInMillis(getTimeInMillis() + other.getTimeInMillis())
                .timeSweepStarted(getTimeSweepStarted())
                .build();
    }

    public static ImmutableSweepResults.Builder builder() {
        return ImmutableSweepResults.builder();
    }

    public static SweepResults createEmptySweepResult() {
        return createEmptySweepResult(Optional.empty());
    }

    public static SweepResults createEmptySweepResult(Optional<byte[]> startRow) {
        return builder()
                .cellTsPairsExamined(0)
                .staleValuesDeleted(0)
                .sweptTimestamp(Long.MAX_VALUE)
                .nextStartRow(startRow)
                .timeInMillis(0)
                .timeSweepStarted(System.currentTimeMillis())
                .build();
    }

}
