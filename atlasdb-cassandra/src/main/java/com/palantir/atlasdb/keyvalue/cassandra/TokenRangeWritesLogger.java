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

import java.time.Duration;
import java.util.List;
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
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.logsafe.SafeArg;
import com.palantir.util.math.Distributions;

public final class TokenRangeWritesLogger {
    private static final Logger log = LoggerFactory.getLogger(TokenRangeWritesLogger.class);

    @VisibleForTesting
    static final long THRESHOLD_WRITES_PER_TABLE = 1_000L;
    @VisibleForTesting
    static final long TIME_UNTIL_LOG_MILLIS = Duration.ofHours(6).toMillis();
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
        if (!newRanges.equals(ranges)) {
            ranges = newRanges;
            statsPerTable.clear();
        }
    }

    public void markWriteForTable(TableReference tableRef, Cell cell) {
        if (AtlasDbConstants.hiddenTables.contains(tableRef)) {
            return;
        }

        TokenRangeWrites tokenRangeWrites = statsPerTable.computeIfAbsent(
                tableRef, t -> new TokenRangeWrites(t, ranges));
        tokenRangeWrites.markWrite(cell);
        tokenRangeWrites.maybeEmitTelemetry();
    }

    private static final class TokenRangeWrites {
        private final TableReference tableRef;
        private final RangeMap<LightweightOppToken, AtomicLong> writesPerRange;
        private final AtomicLong writesSinceLastCalculation = new AtomicLong(0);
        private long lastLoggedTime = System.currentTimeMillis();
        private double probabilityDistributionIsNotUniform = 0.0;
        private long numberOfWritesUsedToCalculateDistribution = 0;

        private TokenRangeWrites(TableReference tableRef, Set<Range<LightweightOppToken>> ranges) {
            this.tableRef = tableRef;
            ImmutableRangeMap.Builder<LightweightOppToken, AtomicLong> builder = ImmutableRangeMap.builder();
            ranges.forEach(range -> builder.put(range, new AtomicLong(0)));
            this.writesPerRange = builder.build();
            new MetricsManager()
                    .registerGaugeForTable(TokenRangeWritesLogger.class, "probabilityDistributionIsNotUniform",
                            tableRef, this::getProbabilityDistributionIsNotUniform);
        }

        private double getProbabilityDistributionIsNotUniform() {
            return probabilityDistributionIsNotUniform;
        }

        private void markWrite(Cell cell) {
            writesPerRange.get(LightweightOppToken.of(cell)).incrementAndGet();
            writesSinceLastCalculation.incrementAndGet();
        }

        private void maybeEmitTelemetry() {
            if (shouldRecalculate()) {
                tryRecalculate();
            }
            if (shouldLog()) {
                tryLog();
            }
        }

        private boolean shouldRecalculate() {
            return writesSinceLastCalculation.get() > THRESHOLD_WRITES_PER_TABLE;
        }

        private synchronized void tryRecalculate() {
            if (shouldRecalculate()) {
                List<Long> values = writesPerRange.asMapOfRanges().values().stream().map(AtomicLong::get)
                        .collect(Collectors.toList());
                numberOfWritesUsedToCalculateDistribution = values.stream().mapToLong(x -> x).sum();
                probabilityDistributionIsNotUniform = Distributions.confidenceThatDistributionIsNotUniform(values);

                writesSinceLastCalculation.set(0);
            }
        }

        private boolean shouldLog() {
            return (System.currentTimeMillis() - lastLoggedTime) > TIME_UNTIL_LOG_MILLIS;
        }

        private synchronized void tryLog() {
            if (shouldLog()) {
                if (probabilityDistributionIsNotUniform > CONFIDENCE_FOR_LOGGING) {
                    logNotUniform();
                } else {
                    logUniform();
                }
                lastLoggedTime = System.currentTimeMillis();
            }
        }

        private void logNotUniform() {
            log.warn("The distribution of writes over token ranges for table {} appears to be significantly "
                    + "skewed: {}.",
                    LoggingArgs.tableRef(tableRef),
                    SafeArg.of("writesPerTokenRange", CassandraLogHelper.tokenRangesToWrites(writesPerRange)));
        }

        private void logUniform() {
            log.info("The distribution of writes over token ranges does not appear to be skewed"
                            + " based on {} writes to table {}.",
                    SafeArg.of("numberOfWrites", numberOfWritesUsedToCalculateDistribution),
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
        return statsPerTable.get(tableRef).numberOfWritesUsedToCalculateDistribution
                + statsPerTable.get(tableRef).writesSinceLastCalculation.get();
    }
}
