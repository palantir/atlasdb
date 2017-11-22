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

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.SafeArg;

import uk.org.lidalia.slf4jext.Level;
import uk.org.lidalia.slf4jtest.LoggingEvent;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

public class TokenRangeWritesLoggerTest {
    private TestLogger infoLogger;
    private TokenRangeWritesLogger writesLogger;

    private static final long TIME_UNTIL_LOG_MILLIS = TokenRangeWritesLogger.TIME_UNTIL_LOG_MILLIS;
    private static final long THRESHOLD_WRITES_PER_TABLE = TokenRangeWritesLogger.THRESHOLD_WRITES_PER_TABLE;
    private static final TableReference TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("test.test");
    private static final TableReference TABLE_REFERENCE2 = TableReference.createFromFullyQualifiedName("test.test2");

    private static final LightweightOppToken BCD = new LightweightOppToken(PtBytes.toBytes("bcd"));
    private static final LightweightOppToken GHI = new LightweightOppToken(PtBytes.toBytes("ghi"));
    private static final LightweightOppToken OPQ = new LightweightOppToken(PtBytes.toBytes("opq"));

    private static final Range<LightweightOppToken> RANGE_1 = Range.atMost(BCD);
    private static final Range<LightweightOppToken> RANGE_2 = Range.openClosed(BCD, GHI);
    private static final Range<LightweightOppToken> RANGE_3 = Range.openClosed(GHI, OPQ);
    private static final Range<LightweightOppToken> RANGE_4 = Range.greaterThan(OPQ);
    private static final Set<Range<LightweightOppToken>> TOKEN_RANGES = ImmutableSet.of(
            RANGE_1, RANGE_2, RANGE_3, RANGE_4);

    @Before
    public void setup() {
        writesLogger = TokenRangeWritesLogger.createUninitialized();
        writesLogger.updateTokenRanges(TOKEN_RANGES);
        infoLogger = TestLoggerFactory.getTestLogger(TokenRangeWritesLogger.class);
        infoLogger.setEnabledLevelsForAllThreads(Level.INFO, Level.WARN);
    }

    @After
    public void tearDown() {
        infoLogger.clearAll();
    }

    @Test
    public void markGetsMarkedInEachRange() {
        writesLogger.markWritesForTable(writesPerRange(1, 1, 1, 1), TABLE_REFERENCE);

        assertWritesPerRangeForTable(1, 1, 1, 1, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(4, TABLE_REFERENCE);
        Assert.assertThat(infoLogger.getAllLoggingEvents().size(), Matchers.equalTo(0));
    }

    @Test
    public void markAddsWritesToSameRangeAndDifferentRow() {
        writesLogger.markWritesForTable(writesPerRange(10, 0, 0, 0), TABLE_REFERENCE);

        assertWritesPerRangeForTable(10, 0, 0, 0, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(10, TABLE_REFERENCE);
        Assert.assertThat(infoLogger.getAllLoggingEvents().size(), Matchers.equalTo(0));
    }

    @Test
    public void markAccumulatesWrites() {
        writesLogger.markWritesForTable(writesPerRange(1, 0, 10, 0), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(0, 1, 10, 0), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(0, 0, 10, 1), TABLE_REFERENCE);

        assertWritesPerRangeForTable(1, 1, 30, 1, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(33, TABLE_REFERENCE);
        Assert.assertThat(infoLogger.getAllLoggingEvents().size(), Matchers.equalTo(0));
    }

    @Test
    public void markGetsMarkedSeparatelyForDifferentTables() {
        writesLogger.markWritesForTable(writesPerRange(0, 0, 20, 0), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(1, 0, 0, 1), TABLE_REFERENCE2);

        assertWritesPerRangeForTable(0, 0, 20, 0, TABLE_REFERENCE);
        assertWritesPerRangeForTable(1, 0, 0, 1, TABLE_REFERENCE2);
        assertTotalNumberOfWritesForTable(20, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(2, TABLE_REFERENCE2);
        Assert.assertThat(infoLogger.getAllLoggingEvents().size(), Matchers.equalTo(0));
    }

    @Test
    public void markResetsNumberOfWritesAndLogsNotUniform() {
        writesLogger.markWritesForTable(writesPerRange(0, 1, 0, 0), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(THRESHOLD_WRITES_PER_TABLE, 1, 0, 4),
                TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(0, 1, 0, 0), TABLE_REFERENCE);

        assertWritesPerRangeForTable(THRESHOLD_WRITES_PER_TABLE, 3, 0, 4, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(1, TABLE_REFERENCE);

        assertLoggedNotUniform(THRESHOLD_WRITES_PER_TABLE, 2, 0, 4, TABLE_REFERENCE);
    }

    @Test
    public void markResetsNumberOfWritesAndLogsUniform() {
        writesLogger.markWritesForTable(writesPerRange(0, 1, 0, 0), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(THRESHOLD_WRITES_PER_TABLE,
                THRESHOLD_WRITES_PER_TABLE,
                THRESHOLD_WRITES_PER_TABLE,
                THRESHOLD_WRITES_PER_TABLE),
                TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(0, 1, 0, 0), TABLE_REFERENCE);

        assertWritesPerRangeForTable(THRESHOLD_WRITES_PER_TABLE,
                THRESHOLD_WRITES_PER_TABLE + 2,
                THRESHOLD_WRITES_PER_TABLE,
                THRESHOLD_WRITES_PER_TABLE,
                TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(1, TABLE_REFERENCE);

        assertLoggedMaybeUniform(4 * THRESHOLD_WRITES_PER_TABLE + 1, true);
    }

    @Test
    public void markLogsInsufficientWritesAfterDayWithNotEnoughWritesAndDoesNotResetCount() {
        writesLogger.markWritesForTable(writesPerRange(0, 1, 0, 0), TABLE_REFERENCE);
        writesLogger.setLastLoggedTime(TABLE_REFERENCE, System.currentTimeMillis() - TIME_UNTIL_LOG_MILLIS - 5);
        writesLogger.markWritesForTable(writesPerRange(100, 0, 0, 5), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(0, 1, 0, 0), TABLE_REFERENCE);

        assertWritesPerRangeForTable(100, 2, 0, 5, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(107, TABLE_REFERENCE);

        assertLoggedMaybeUniform(106, false);
    }

    @Test
    public void markLogsNotUniformAndResetsTimeIfADayHasPassed() {
        writesLogger.markWritesForTable(writesPerRange(0, 1, 0, 0), TABLE_REFERENCE);
        writesLogger.setLastLoggedTime(TABLE_REFERENCE, System.currentTimeMillis() - TIME_UNTIL_LOG_MILLIS - 5);
        writesLogger.markWritesForTable(writesPerRange(THRESHOLD_WRITES_PER_TABLE, 1, 0, 4),
                TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(0, 1, 0, 0), TABLE_REFERENCE);

        assertWritesPerRangeForTable(THRESHOLD_WRITES_PER_TABLE, 3, 0, 4, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(1, TABLE_REFERENCE);
        Assert.assertThat(writesLogger.getLastLoggedTime(TABLE_REFERENCE),
                Matchers.greaterThan(System.currentTimeMillis() - 1000L));

        assertLoggedNotUniform(THRESHOLD_WRITES_PER_TABLE, 2, 0, 4, TABLE_REFERENCE);
    }

    @Test
    public void markLogsOnlyForTableThatExceedsThreshold() {
        writesLogger.markWritesForTable(writesPerRange(1, 2, 0, 4), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(2, 2, 2, 2), TABLE_REFERENCE2);
        writesLogger.markWritesForTable(writesPerRange(2, 2, 2, 2), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(THRESHOLD_WRITES_PER_TABLE, 2, 2, 2), TABLE_REFERENCE2);
        writesLogger.markWritesForTable(writesPerRange(2, 2, 2, 2), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(2, 2, 2, 2), TABLE_REFERENCE2);

        assertWritesPerRangeForTable(5, 6, 4, 8, TABLE_REFERENCE);
        assertWritesPerRangeForTable(THRESHOLD_WRITES_PER_TABLE + 4, 6, 6, 6, TABLE_REFERENCE2);
        assertTotalNumberOfWritesForTable(23, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(8, TABLE_REFERENCE2);

        assertLoggedNotUniform(THRESHOLD_WRITES_PER_TABLE + 2, 4, 4, 4, TABLE_REFERENCE2);
    }

    @Test
    public void testUpdateDoesNotResetCountsWhenSameRanges() {
        writesLogger.markWritesForTable(writesPerRange(0, 1, 0, 0), TABLE_REFERENCE);
        writesLogger.updateTokenRanges(ImmutableSet.of(RANGE_2, RANGE_1, RANGE_4, RANGE_3));
        writesLogger.markWritesForTable(writesPerRange(2, 2, 2, 2), TABLE_REFERENCE);

        assertWritesPerRangeForTable(2, 3, 2, 2, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(9, TABLE_REFERENCE);
    }

    @Test
    public void testUpdateResetsCountsWhenNewRanges() {
        writesLogger.markWritesForTable(writesPerRange(0, 1, 0, 0), TABLE_REFERENCE);
        writesLogger.updateTokenRanges(ImmutableSet.of(Range.all()));
        writesLogger.markWritesForTable(writesPerRange(2, 2, 2, 2), TABLE_REFERENCE);

        assertWritesForRangeAndTable(8, Range.all(), TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(8, TABLE_REFERENCE);
    }

    private Set<Map.Entry<Cell, Integer>> writesPerRange(long fst, long snd, long trd, long fth) {
        ImmutableMap.Builder<Cell, Integer> builder = ImmutableMap.builder();
        builder = writesWithRownamePrefix("a", fst, builder);
        builder = writesWithRownamePrefix("e", snd, builder);
        builder = writesWithRownamePrefix("k", trd, builder);
        builder = writesWithRownamePrefix("s", fth, builder);
        return builder.build().entrySet();
    }

    private ImmutableMap.Builder<Cell, Integer> writesWithRownamePrefix(String prefix, long number,
            ImmutableMap.Builder<Cell, Integer> builder) {
        for (long i = 0; i < number; i++) {
            builder.put(Cell.create(PtBytes.toBytes(prefix + Long.toString(i)), PtBytes.toBytes("a")), 1);
        }
        return builder;
    }

    private void assertWritesPerRangeForTable(long fst, long snd, long trd, long fth, TableReference tableRef) {
        assertWritesForRangeAndTable(fst, RANGE_1, tableRef);
        assertWritesForRangeAndTable(snd, RANGE_2, tableRef);
        assertWritesForRangeAndTable(trd, RANGE_3, tableRef);
        assertWritesForRangeAndTable(fth, RANGE_4, tableRef);
    }

    private void assertWritesForRangeAndTable(long number, Range<LightweightOppToken> range, TableReference tableRef) {
        Assert.assertThat(writesLogger.getNumberOfWritesInRange(tableRef, range), Matchers.equalTo(number));
    }

    private void assertTotalNumberOfWritesForTable(long number, TableReference tableRef) {
        Assert.assertThat(writesLogger.getNumberOfWritesTotal(tableRef), Matchers.equalTo(number));
    }

    @SuppressWarnings("unchecked")
    private void assertLoggedNotUniform(long fst, long snd, long trd, long fth, TableReference tableRef) {
        LoggingEvent loggingEvent = getLoggingEventAndAssertLoggedAtLevel(Level.WARN);
        Assert.assertThat(loggingEvent.getArguments().get(0), Matchers.equalTo(LoggingArgs.tableRef(tableRef)));
        SafeArg<List<String>> argument = (SafeArg<List<String>>) loggingEvent.getArguments().get(1);
        Assert.assertThat(argument.getValue(), Matchers.containsInAnyOrder(
                "range from (no lower bound) to 626364 has " + fst + " writes",
                "range from 626364 to 676869 has " + snd + " writes",
                "range from 676869 to 6F7071 has " + trd + " writes",
                "range from 6F7071 to (no upper bound) has " + fth + " writes"));
    }

    private void assertLoggedMaybeUniform(long numWrites, boolean enoughWrites) {
        LoggingEvent loggingEvent = getLoggingEventAndAssertLoggedAtLevel(Level.INFO);
        Assert.assertThat(loggingEvent.getArguments().get(0),
                Matchers.equalTo(SafeArg.of("numberOfWrites", numWrites)));
        Assert.assertThat(loggingEvent.getArguments().get(1), Matchers.equalTo(LoggingArgs.tableRef(TABLE_REFERENCE)));
        if (!enoughWrites) {
            Assert.assertThat(loggingEvent.getArguments().get(2), Matchers.equalTo(
                    SafeArg.of("thresholdOfWrites", THRESHOLD_WRITES_PER_TABLE)));
        }
    }

    private LoggingEvent getLoggingEventAndAssertLoggedAtLevel(Level level) {
        Assert.assertThat(infoLogger.getAllLoggingEvents().size(), Matchers.equalTo(1));
        LoggingEvent loggingEvent = infoLogger.getAllLoggingEvents().asList().get(0);
        Assert.assertThat(loggingEvent.getLevel(), Matchers.equalTo(level));
        return loggingEvent;
    }
}
