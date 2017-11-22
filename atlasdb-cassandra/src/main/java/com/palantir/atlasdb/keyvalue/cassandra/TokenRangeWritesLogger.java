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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
import com.palantir.util.MathUtils;

public final class TokenRangeWritesLogger {
    private static final Logger log = LoggerFactory.getLogger(TokenRangeWritesLogger.class);

    @VisibleForTesting
    static final long THRESHOLD_WRITES_PER_TABLE = 1_000_000L;
    @VisibleForTesting
    static final long TIME_UNTIL_LOG_MILLIS = 24 * 60 * 60 * 1000L;
    private static final double CONFIDENCE_FOR_LOGGING = 0.99;

    private final ConcurrentMap<TableReference, TokenRangeWrites> statsPerTable = new ConcurrentHashMap<>();
    private volatile Set<Range<LightweightOppToken>> ranges = ImmutableSet.of(Range.all());

    private TokenRangeWritesLogger() {
        // use factory method
    }

    public static TokenRangeWritesLogger createUninitialized() {
        return new TokenRangeWritesLogger();
    }

    public void updateTokenRanges(Set<Range<LightweightOppToken>> newRanges) {
        Preconditions.checkArgument(!newRanges.isEmpty(), "Set of ranges must not be empty!");
        if (!Sets.symmetricDifference(ranges, newRanges).isEmpty()) {
            ranges = newRanges;
            statsPerTable.clear();
        }
    }

    <V> void  markWritesForTable(Set<Map.Entry<Cell, V>> entries, TableReference tableRef) {
        statsPerTable.putIfAbsent(tableRef, new TokenRangeWrites(tableRef, ranges));
        TokenRangeWrites tokenRangeWrites = statsPerTable.get(tableRef);
        entries.forEach(entry -> tokenRangeWrites.markWrite(entry.getKey()));
        tokenRangeWrites.maybeLog();
    }

    private static final class TokenRangeWrites {
        private final TableReference tableRef;
        private final RangeMap<LightweightOppToken, AtomicLong> writesPerRange;
        private final AtomicLong totalWrites = new AtomicLong(0);
        private long lastLoggedTime = System.currentTimeMillis();

        private TokenRangeWrites(TableReference tableRef, Set<Range<LightweightOppToken>> ranges) {
            this.tableRef = tableRef;
            ImmutableRangeMap.Builder<LightweightOppToken, AtomicLong> builder = ImmutableRangeMap.builder();
            ranges.forEach(range -> builder.put(range, new AtomicLong(0)));
            this.writesPerRange = builder.build();
        }

        private void markWrite(Cell cell) {
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
                    lastLoggedTime = System.currentTimeMillis();
                    return;
                }
                if (distributionNotUniform()) {
                    logNotUniform();
                } else {
                    logUniform(numWrites);
                }
                totalWrites.set(0);
                lastLoggedTime = System.currentTimeMillis();
            }
        }

        private boolean shouldLog() {
            return exceedsThresholdOfWrites(totalWrites.get())
                    || (System.currentTimeMillis() - lastLoggedTime) > TIME_UNTIL_LOG_MILLIS;
        }

        private boolean exceedsThresholdOfWrites(long numWrites) {
            return numWrites > THRESHOLD_WRITES_PER_TABLE;
        }

        private boolean distributionNotUniform() {
            List<Long> values = writesPerRange.asMapOfRanges().values().stream().map(AtomicLong::get)
                    .collect(Collectors.toList());
            return MathUtils.confidenceDistributionIsNotUniform(values) > CONFIDENCE_FOR_LOGGING;
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

    @VisibleForTesting
    long getLastLoggedTime(TableReference tableRef) {
        return statsPerTable.get(tableRef).lastLoggedTime;
    }

    @VisibleForTesting
    void setLastLoggedTime(TableReference tableRef, long newTime) {
        statsPerTable.get(tableRef).lastLoggedTime = newTime;
    }

    @VisibleForTesting
    long getNumberOfWritesFromToken(TableReference tableRef, LightweightOppToken token) {
        return statsPerTable.get(tableRef).writesPerRange.get(token).get();
    }

    @VisibleForTesting
    long getNumberOfWritesInRange(TableReference tableRef, Range<LightweightOppToken> range) {
        return statsPerTable.get(tableRef).writesPerRange.asMapOfRanges().get(range).get();
    }

    @VisibleForTesting
    long getNumberOfWritesTotal(TableReference tableRef) {
        return statsPerTable.get(tableRef).totalWrites.get();
    }
}
