/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableRangeMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.SafeArg;

public class TokenRangeWritesLogger {
    private static final Logger log = LoggerFactory.getLogger(TokenRangeWritesLogger.class);

    @VisibleForTesting
    static final long THRESHOLD_WRITES_PER_TABLE = 1_000_000L;
    static final long TIME_UNTIL_LOG_MILLIS = 24 * 60 * 60 * 1000L;

    ConcurrentMap<TableReference, TokenRangeWrites> statsPerTable = new ConcurrentHashMap<>();
    volatile Set<Range<LightweightOppToken>> ranges;

    public TokenRangeWritesLogger(Set<Range<LightweightOppToken>> ranges) {
        Preconditions.checkArgument(!ranges.isEmpty(), "Set of ranges must not be empty!");
        this.ranges = ranges;
    }

    public static TokenRangeWritesLogger createUninitialized() {
        return new TokenRangeWritesLogger(ImmutableSet.of(Range.all()));
    }

    public void update(Set<Range<LightweightOppToken>> newRanges) {
        Preconditions.checkArgument(!newRanges.isEmpty(), "Set of ranges must not be empty!");
        if (!Sets.symmetricDifference(ranges, newRanges).isEmpty()) {
            ranges = newRanges;
            statsPerTable.clear();
        }
    }

    public <V> void  markWritesForTable(Set<Map.Entry<Cell, V>> entries, TableReference tableRef) {
        statsPerTable.putIfAbsent(tableRef, new TokenRangeWrites(tableRef, ranges));
        TokenRangeWrites tokenRangeWrites = statsPerTable.get(tableRef);
        entries.forEach(entry -> tokenRangeWrites.markWrite(entry.getKey()));
        tokenRangeWrites.maybeLog();
    }

    static class TokenRangeWrites {
        final TableReference tableRef;
        final RangeMap<LightweightOppToken, AtomicLong> writesPerRange;
        final AtomicLong totalWrites = new AtomicLong(0);
        long lastReportedTime = System.currentTimeMillis();

        TokenRangeWrites(TableReference tableRef, Set<Range<LightweightOppToken>> ranges) {
            this.tableRef = tableRef;
            ImmutableRangeMap.Builder<LightweightOppToken, AtomicLong> builder = ImmutableRangeMap.builder();
            ranges.forEach(range -> builder.put(range, new AtomicLong(0)));
            this.writesPerRange = builder.build();
        }

        public void markWrite(Cell cell) {
            writesPerRange.get(LightweightOppToken.of(cell)).incrementAndGet();
            totalWrites.incrementAndGet();
        }

        private void maybeLog() {
            if (shouldLog()) {
                tryLog();
            }
        }

        private synchronized void tryLog() {
            if (shouldLog()) {
                long numWrites = totalWrites.get();
                if (!exceedsThresholdOfWrites(numWrites)) {
                    logNotEnoughWrites(numWrites);
                    lastReportedTime = System.currentTimeMillis();
                    return;
                }
                if (distributionNotUniform()) {
                    logNotUniform();
                } else {
                    logUniform(numWrites);
                }
                totalWrites.set(0);
                lastReportedTime = System.currentTimeMillis();
            }
        }

        private boolean shouldLog() {
            return exceedsThresholdOfWrites(totalWrites.get())
                    || (System.currentTimeMillis() - lastReportedTime) > TIME_UNTIL_LOG_MILLIS;
        }

        private boolean exceedsThresholdOfWrites(long numWrites) {
            return numWrites > THRESHOLD_WRITES_PER_TABLE;
        }

        private boolean distributionNotUniform() {
            return true;
        }

        private void logNotEnoughWrites(long numWrites) {
            log.info("There were {} writes into the table {} since the last statistical analysis, which is less "
                            + "than the required threshold of {} writes.",
                    SafeArg.of("numberOfWrites", numWrites),
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("thresholdOfWrites", THRESHOLD_WRITES_PER_TABLE));
        }

        private void logNotUniform() {
            log.warn("The distribution of writes over token ranges for table {} appears to be significantly "
                    + "skewed: {}.",
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("writesPerTokenRange", CassandraLogHelper.tokenRangesToWrites(writesPerRange)));
        }

        private void logUniform(long numWrites) {
            log.info("There were at least {} writes into the table {} since the last statistical analysis. The "
                            + "distribution of writes over token ranges does not appear to be skewed.",
                    SafeArg.of("numberOfWrites", numWrites),
                    LoggingArgs.tableRef(tableRef));
        }
    }
}
