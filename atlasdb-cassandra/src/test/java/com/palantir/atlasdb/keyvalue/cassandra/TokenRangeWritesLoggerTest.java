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
import java.util.List;
import java.util.Set;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.logsafe.SafeArg;

import uk.org.lidalia.slf4jext.Level;
import uk.org.lidalia.slf4jtest.LoggingEvent;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

public class TokenRangeWritesLoggerTest {
    private TestLogger infoLogger;
    private TokenRangeWritesLogger writesLogger;

    private static final long TIME_UNTIL_LOG_MILLIS = TokenRangeWritesLogger.TIME_UNTIL_LOG_MILLIS;
    private static final long LIMIT = TokenRangeWritesLogger.THRESHOLD_WRITES_PER_TABLE;
    private static final long QUARTER = LIMIT / 4;
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
        writesLogger = TokenRangeWritesLogger.createUninitialized(
                MetricsManagers.createForTests());
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
        markWritesPerRangeForTable(1, 1, 1, 1, TABLE_REFERENCE);

        assertWritesPerRangeForTable(1, 1, 1, 1, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(4, TABLE_REFERENCE);
        assertNoLogging();
    }

    @Test
    public void markAddsWritesToSameRangeAndDifferentRow() {
        markWritesPerRangeForTable(10, 0, 0, 0, TABLE_REFERENCE);

        assertWritesPerRangeForTable(10, 0, 0, 0, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(10, TABLE_REFERENCE);
        assertNoLogging();
    }

    @Test
    public void markAccumulatesWrites() {
        markWritesPerRangeForTable(1, 0, 10, 0, TABLE_REFERENCE);
        markWritesPerRangeForTable(0, 1, 10, 0, TABLE_REFERENCE);
        markWritesPerRangeForTable(0, 0, 10, 1, TABLE_REFERENCE);

        assertWritesPerRangeForTable(1, 1, 30, 1, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(33, TABLE_REFERENCE);
        assertNoLogging();
    }

    @Test
    public void markGetsMarkedSeparatelyForDifferentTables() {
        markWritesPerRangeForTable(0, 0, 20, 0, TABLE_REFERENCE);
        markWritesPerRangeForTable(1, 0, 0, 1, TABLE_REFERENCE2);

        assertWritesPerRangeForTable(0, 0, 20, 0, TABLE_REFERENCE);
        assertWritesPerRangeForTable(1, 0, 0, 1, TABLE_REFERENCE2);
        assertTotalNumberOfWritesForTable(20, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(2, TABLE_REFERENCE2);
        assertNoLogging();
    }

    @Test
    public void markDoesNotLogIfNotEnoughTimeHasPassed() {
        markWritesPerRangeForTable(0, 1, 0, 0, TABLE_REFERENCE);
        markWritesPerRangeForTable(LIMIT, 1, 0, 4, TABLE_REFERENCE);
        markWritesPerRangeForTable(0, 1, 0, 0, TABLE_REFERENCE);

        assertWritesPerRangeForTable(LIMIT, 3, 0, 4, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(LIMIT + 7, TABLE_REFERENCE);
        assertNoLogging();
    }

    @Test
    public void markLogsWhenTimeElapsedButFewWrites() {
        markWritesPerRangeForTable(0, 1, 0, 0, TABLE_REFERENCE);
        long lastLoggedTime = setLastLoggedTimeBeyondLoggingWindow(TABLE_REFERENCE);
        markWritesPerRangeForTable(0, 1, 0, 0, TABLE_REFERENCE);

        assertWritesPerRangeForTable(0, 2, 0, 0, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(2, TABLE_REFERENCE);
        assertLastLoggedTimeEquals(TABLE_REFERENCE, lastLoggedTime);
        assertLoggedUniform(0);
    }

    @Test
    public void markResetsNumberOfWritesAndTimeAndLogsNotUniform() {
        markWritesPerRangeForTable(0, 1, 0, 0, TABLE_REFERENCE);
        setLastLoggedTimeBeyondLoggingWindow(TABLE_REFERENCE);
        markWritesPerRangeForTable(LIMIT, 1, 0, 4, TABLE_REFERENCE);
        markWritesPerRangeForTable(0, 1, 0, 0, TABLE_REFERENCE);

        assertWritesPerRangeForTable(LIMIT, 3, 0, 4, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(LIMIT + 7, TABLE_REFERENCE);
        assertLastLoggedTimeWithinDeltaOfCurrent(TABLE_REFERENCE);
        assertLoggedNotUniform(LIMIT, 2, 0, 4, TABLE_REFERENCE);
    }

    @Test
    public void markLogsWhenEnoughTimePassesIfEnoughWritesInPast() {
        markWritesPerRangeForTable(0, 1, 0, 0, TABLE_REFERENCE);
        markWritesPerRangeForTable(LIMIT, 1, 0, 4, TABLE_REFERENCE);
        setLastLoggedTimeBeyondLoggingWindow(TABLE_REFERENCE);
        markWritesPerRangeForTable(0, 1, 0, 0, TABLE_REFERENCE);

        assertWritesPerRangeForTable(LIMIT, 3, 0, 4, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(LIMIT + 7, TABLE_REFERENCE);
        assertLastLoggedTimeWithinDeltaOfCurrent(TABLE_REFERENCE);
        assertLoggedNotUniform(LIMIT, 3, 0, 4, TABLE_REFERENCE);
    }

    @Test
    public void markResetsNumberOfWritesAndTimeAndLogsUniform() {
        markWritesPerRangeForTable(3, 3, 3, 3, TABLE_REFERENCE);
        setLastLoggedTimeBeyondLoggingWindow(TABLE_REFERENCE);
        markWritesPerRangeForTable(QUARTER, QUARTER, QUARTER, QUARTER, TABLE_REFERENCE);
        markWritesPerRangeForTable(0, 1, 0, 0, TABLE_REFERENCE);

        assertWritesPerRangeForTable(QUARTER + 3, QUARTER + 4, QUARTER + 3, QUARTER + 3, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(1013, TABLE_REFERENCE);
        assertLastLoggedTimeWithinDeltaOfCurrent(TABLE_REFERENCE);
        assertLoggedUniform(4 * QUARTER + 12);
    }

    @Test
    public void testUpdateDoesNotResetCountsWhenSameRanges() {
        markWritesPerRangeForTable(0, 1, 0, 0, TABLE_REFERENCE);
        long lastLoggedTime = setLastLoggedTimeBeyondLoggingWindow(TABLE_REFERENCE);
        writesLogger.updateTokenRanges(ImmutableSet.of(RANGE_2, RANGE_1, RANGE_4, RANGE_3));
        markWritesPerRangeForTable(2, 2, 2, 2, TABLE_REFERENCE);

        assertWritesPerRangeForTable(2, 3, 2, 2, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(9, TABLE_REFERENCE);
        assertLastLoggedTimeEquals(TABLE_REFERENCE, lastLoggedTime);
    }

    @Test
    public void testUpdateResetsCountsAndTimeWhenNewRanges() {
        markWritesPerRangeForTable(0, 1, 0, 0, TABLE_REFERENCE);
        setLastLoggedTimeBeyondLoggingWindow(TABLE_REFERENCE);
        writesLogger.updateTokenRanges(ImmutableSet.of(Range.all()));
        markWritesPerRangeForTable(2, 2, 2, 2, TABLE_REFERENCE);

        assertWritesForRangeAndTable(8, Range.all(), TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(8, TABLE_REFERENCE);
        assertLastLoggedTimeWithinDeltaOfCurrent(TABLE_REFERENCE);
        assertNoLogging();
    }

    private void markWritesPerRangeForTable(long fst, long snd, long trd, long fth, TableReference tableRef) {
        writesLogger.markWritesForTable(writesPerRange(fst, snd, trd, fth), tableRef);
    }

    private Set<Cell> writesPerRange(long fst, long snd, long trd, long fth) {
        Set<Cell> result = new HashSet<>();
        addWritesWithRownamePrefix("a", fst, result);
        addWritesWithRownamePrefix("e", snd, result);
        addWritesWithRownamePrefix("k", trd, result);
        addWritesWithRownamePrefix("s", fth, result);
        return result;
    }

    private void addWritesWithRownamePrefix(String prefix, long number, Set<Cell> accumulator) {
        for (long i = 0; i < number; i++) {
            accumulator.add(Cell.create(PtBytes.toBytes(prefix + Long.toString(i)), PtBytes.toBytes("a")));
        }
    }

    private long setLastLoggedTimeBeyondLoggingWindow(TableReference tableRef) {
        long newTime = System.currentTimeMillis() - TIME_UNTIL_LOG_MILLIS - 5;
        writesLogger.setLastLoggedTime(tableRef, newTime);
        return newTime;
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

    private void assertLastLoggedTimeEquals(TableReference tableRef, long lastLoggedTime) {
        Assert.assertThat(writesLogger.getLastLoggedTime(tableRef),
                Matchers.greaterThan(lastLoggedTime - 1000L));
    }

    private void assertLastLoggedTimeWithinDeltaOfCurrent(TableReference tableRef) {
        Assert.assertThat(writesLogger.getLastLoggedTime(tableRef),
                Matchers.greaterThan(System.currentTimeMillis() - 1000L));
    }

    private void assertNoLogging() {
        Assert.assertThat(infoLogger.getAllLoggingEvents().size(), Matchers.equalTo(0));
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

    private void assertLoggedUniform(long numWrites) {
        LoggingEvent loggingEvent = getLoggingEventAndAssertLoggedAtLevel(Level.INFO);
        Assert.assertThat(loggingEvent.getArguments().get(0),
                Matchers.equalTo(SafeArg.of("numberOfWrites", numWrites)));
        Assert.assertThat(loggingEvent.getArguments().get(1), Matchers.equalTo(LoggingArgs.tableRef(TABLE_REFERENCE)));
    }

    private LoggingEvent getLoggingEventAndAssertLoggedAtLevel(Level level) {
        Assert.assertThat(infoLogger.getAllLoggingEvents().size(), Matchers.equalTo(1));
        LoggingEvent loggingEvent = infoLogger.getAllLoggingEvents().asList().get(0);
        Assert.assertThat(loggingEvent.getLevel(), Matchers.equalTo(level));
        return loggingEvent;
    }
}
