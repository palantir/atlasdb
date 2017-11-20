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
import org.mockito.Mockito;

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
    private TestLogger logger;

    private CassandraClientPoolImpl clientPool = Mockito.mock(CassandraClientPoolImpl.class);
    private TokenRangeWritesLogger writesLogger;

    private static final TableReference TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("test.test");
    private static final TableReference TABLE_REFERENCE2 = TableReference.createFromFullyQualifiedName("test.test2");

    private static final LightweightOppToken BCD = new LightweightOppToken(PtBytes.toBytes("bcd"));
    private static final LightweightOppToken GHI = new LightweightOppToken(PtBytes.toBytes("ghi"));
    private static final LightweightOppToken OPQ = new LightweightOppToken(PtBytes.toBytes("opq"));

    private static final Range<LightweightOppToken> RANGE_1 = Range.lessThan(BCD);
    private static final Range<LightweightOppToken> RANGE_2 = Range.openClosed(BCD, GHI);
    private static final Range<LightweightOppToken> RANGE_3 = Range.openClosed(GHI, OPQ);
    private static final Range<LightweightOppToken> RANGE_4 = Range.greaterThan(OPQ);
    private static final Set<Range<LightweightOppToken>> TOKEN_RANGES = ImmutableSet.of(
            RANGE_1, RANGE_2, RANGE_3, RANGE_4);

    @Before
    public void setup() {
        Mockito.when(clientPool.getTokenRanges()).thenReturn(TOKEN_RANGES);
        writesLogger  = TokenRangeWritesLogger.createFromClientPool(clientPool);
        logger = TestLoggerFactory.getTestLogger(TokenRangeWritesLogger.class);
        logger.setEnabledLevelsForAllThreads(Level.INFO);
    }

    @After
    public void tearDown() {
        logger.clearAll();
    }

    @Test
    public void markGetsMarkedInEachRange() {
        writesLogger.markWritesForTable(writesPerRange(1, 1, 1, 1), TABLE_REFERENCE);

        assertWritesPerRangeForTable(1, 1, 1, 1, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(4, TABLE_REFERENCE);
        Assert.assertThat(logger.getAllLoggingEvents().size(), Matchers.equalTo(0));
    }

    @Test
    public void markAddsWritesToSameRangeAndDifferentRow() {
        writesLogger.markWritesForTable(writesPerRange(10, 0, 0, 0), TABLE_REFERENCE);

        assertWritesPerRangeForTable(10, 0, 0, 0, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(10, TABLE_REFERENCE);
        Assert.assertThat(logger.getAllLoggingEvents().size(), Matchers.equalTo(0));
    }

    @Test
    public void markAccumulatesWrites() {
        writesLogger.markWritesForTable(writesPerRange(1, 0, 10, 0), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(0, 1, 10, 0), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(0, 0, 10, 1), TABLE_REFERENCE);

        assertWritesPerRangeForTable(1, 1, 30, 1, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(33, TABLE_REFERENCE);
        Assert.assertThat(logger.getAllLoggingEvents().size(), Matchers.equalTo(0));
    }

    @Test
    public void markGetsMarkedSeparatelyForDifferentTables() {
        writesLogger.markWritesForTable(writesPerRange(0, 0, 20, 0), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(1, 0, 0, 1), TABLE_REFERENCE2);

        assertWritesPerRangeForTable(0, 0, 20, 0, TABLE_REFERENCE);
        assertWritesPerRangeForTable(1, 0, 0, 1, TABLE_REFERENCE2);
        assertTotalNumberOfWritesForTable(20, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(2, TABLE_REFERENCE2);
        Assert.assertThat(logger.getAllLoggingEvents().size(), Matchers.equalTo(0));
    }

    @Test
    public void markResetsNumberOfWritesAndLogsWhenItExceedsThreshold() {
        writesLogger.writesPerTable.putIfAbsent(TABLE_REFERENCE, TokenRangeWritesLogger.THRESHOLD_WRITES_PER_TABLE - 1);
        writesLogger.markWritesForTable(writesPerRange(1, 2, 0, 4), TABLE_REFERENCE);

        assertWritesPerRangeForTable(1, 2, 0, 4, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(0, TABLE_REFERENCE);

        Assert.assertThat(logger.getAllLoggingEvents().size(), Matchers.equalTo(1));
        LoggingEvent test = logger.getAllLoggingEvents().asList().get(0);
        Assert.assertThat(test.getArguments().get(0), Matchers.equalTo(LoggingArgs.tableRef(TABLE_REFERENCE)));
        SafeArg<List<String>> argument = (SafeArg<List<String>>) test.getArguments().get(1);
        Assert.assertThat(argument.getValue(), Matchers.containsInAnyOrder(
                "range from (no lower bound) to 626364 has 1",
                "range from 626364 to 676869 has 2",
                "range from 6F7071 to (no upper bound) has 4"));
    }

    @Test
    public void markLogsOnlyForTableThatExceedsThreshold() {
        writesLogger.writesPerTable.putIfAbsent(TABLE_REFERENCE, TokenRangeWritesLogger.THRESHOLD_WRITES_PER_TABLE - 9);
        writesLogger.markWritesForTable(writesPerRange(1, 2, 0, 4), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(2, 2, 2, 2), TABLE_REFERENCE2);
        writesLogger.markWritesForTable(writesPerRange(2, 2, 2, 2), TABLE_REFERENCE);
        writesLogger.markWritesForTable(writesPerRange(2, 2, 2, 2), TABLE_REFERENCE2);
        writesLogger.markWritesForTable(writesPerRange(2, 2, 2, 2), TABLE_REFERENCE);

        assertWritesPerRangeForTable(5, 6, 4, 8, TABLE_REFERENCE);
        assertWritesPerRangeForTable(4, 4, 4, 4, TABLE_REFERENCE2);
        assertTotalNumberOfWritesForTable(8, TABLE_REFERENCE);
        assertTotalNumberOfWritesForTable(16, TABLE_REFERENCE2);

        Assert.assertThat(logger.getAllLoggingEvents().size(), Matchers.equalTo(1));
        LoggingEvent test = logger.getAllLoggingEvents().asList().get(0);
        Assert.assertThat(test.getArguments().get(0), Matchers.equalTo(LoggingArgs.tableRef(TABLE_REFERENCE)));
        SafeArg<List<String>> argument = (SafeArg<List<String>>) test.getArguments().get(1);
        Assert.assertThat(argument.getValue(), Matchers.containsInAnyOrder(
                "range from (no lower bound) to 626364 has 3",
                "range from 626364 to 676869 has 4",
                "range from 676869 to 6F7071 has 2",
                "range from 6F7071 to (no upper bound) has 6"));
    }

    private Set<Map.Entry<Cell, Integer>> writesPerRange(int fst, int snd, int trd, int fth) {
        ImmutableMap.Builder<Cell, Integer> builder = ImmutableMap.builder();
        builder = writesWithRownamePrefix("a", fst, builder);
        builder = writesWithRownamePrefix("e", snd, builder);
        builder = writesWithRownamePrefix("k", trd, builder);
        builder = writesWithRownamePrefix("s", fth, builder);
        return builder.build().entrySet();
    }

    private ImmutableMap.Builder<Cell, Integer> writesWithRownamePrefix(String prefix, int number,
            ImmutableMap.Builder<Cell, Integer> builder) {
        for (int i = 0; i < number; i++) {
            builder.put(Cell.create(PtBytes.toBytes(prefix + Integer.toString(i)), PtBytes.toBytes("a")), 1);
        }
        return builder;
    }

    private void assertWritesPerRangeForTable(int fst, int snd, int trd, int fth, TableReference tableRef) {
        assertWritesForRangeAndTable(fst, RANGE_1, tableRef);
        assertWritesForRangeAndTable(snd, RANGE_2, tableRef);
        assertWritesForRangeAndTable(trd, RANGE_3, tableRef);
        assertWritesForRangeAndTable(fth, RANGE_4, tableRef);
    }

    private void assertWritesForRangeAndTable(long number, Range<LightweightOppToken> range, TableReference tableRef) {
        Assert.assertThat(writesLogger.tokenRangeWritesPerTable.asMapOfRanges().get(range).getOrDefault(tableRef, 0L),
                Matchers.equalTo(number));
    }

    private void assertTotalNumberOfWritesForTable(long number, TableReference tableRef) {
        Assert.assertThat(writesLogger.writesPerTable.getOrDefault(tableRef, 0L), Matchers.equalTo(number));
    }
}
