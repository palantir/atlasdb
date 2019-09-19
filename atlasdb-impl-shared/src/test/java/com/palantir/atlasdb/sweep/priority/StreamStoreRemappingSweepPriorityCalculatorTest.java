/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.sweep.priority;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class StreamStoreRemappingSweepPriorityCalculatorTest {
    private static final long THIRTY_MINUTES_AGO = ZonedDateTime.now().minusMinutes(30).toInstant().toEpochMilli();
    private static final long TWO_HOURS_AGO = ZonedDateTime.now().minusHours(2).toInstant().toEpochMilli();
    private static final long TWELVE_HOURS_AGO = ZonedDateTime.now().minusHours(12).toInstant().toEpochMilli();
    private static final long THIRTY_HOURS_AGO = ZonedDateTime.now().minusHours(30).toInstant().toEpochMilli();
    private static final long FIVE_DAYS_AGO = ZonedDateTime.now().minusDays(5).toInstant().toEpochMilli();
    private static final long SIX_DAYS_AGO = ZonedDateTime.now().minusDays(6).toInstant().toEpochMilli();
    private static final long ONE_MONTH_AGO = ZonedDateTime.now().minusMonths(1).toInstant().toEpochMilli();
    private static final long SEVEN_MONTHS_AGO = ZonedDateTime.now().minusMonths(7).toInstant().toEpochMilli();

    private KeyValueService kvs;
    private SweepPriorityStore sweepPriorityStore;

    private StreamStoreRemappingSweepPriorityCalculator calculator;

    private Set<TableReference> allTables;
    private List<SweepPriority> oldPriorities;
    private List<SweepPriority> newPriorities;
    private boolean isCassandra;

    private Map<TableReference, Double> priorities;

    @Before
    public void setup() {
        kvs = mock(KeyValueService.class);
        sweepPriorityStore = mock(SweepPriorityStore.class);

        SweepPriorityCalculator nextTableToSweep = new SweepPriorityCalculator(kvs, sweepPriorityStore);
        calculator = new StreamStoreRemappingSweepPriorityCalculator(nextTableToSweep, sweepPriorityStore);

        allTables = new HashSet<>(AtlasDbConstants.HIDDEN_TABLES);
        oldPriorities = new ArrayList<>();
        newPriorities = new ArrayList<>();
        isCassandra = true;
    }


    @Test
    public void noTables() {
        givenNoTables();

        whenCalculatingSweepPriorities();

        thenNoTablesToSweep();
    }

    @Test
    public void unsweptTableIsAlwaysPrioritised() {
        TableReference unsweptTable = table("unswept1");

        given(unsweptTable);

        whenCalculatingSweepPriorities();

        thenOnlyTablePrioritisedIs(unsweptTable);
        thenTableHasPriority(unsweptTable);
    }

    @Test
    public void tableCleared_isLowPriority() {
        SweepPriorityHistory clearedTable = new SweepPriorityHistory("clearedTable")
                .withOld(sweepPriority().writeCount(100).build())
                .withNew(sweepPriority().writeCount(50).build());

        given(clearedTable);

        whenCalculatingSweepPriorities();

        thenOnlyTablePrioritisedIs(clearedTable);
        thenTableHasZeroPriority(clearedTable);
    }

    @Test
    public void unsweptTableAndNormalTable_prioritiseUnsweptTable() {
        TableReference unsweptTable = table("unswept1");

        SweepPriorityHistory normalTable = new SweepPriorityHistory("normalTable")
                .withOld(sweepPriority().writeCount(50).build())
                .withNew(sweepPriority().writeCount(100).build());

        given(unsweptTable);
        given(normalTable);

        whenCalculatingSweepPriorities();

        thenOnlyTablePrioritisedIs(unsweptTable);
    }

    @Test
    public void tableDidNotChangeMuchLastTimeWeSweptIt_doNotPrioritise() {
        SweepPriorityHistory rarelyUpdatedTable =
                new SweepPriorityHistory("rarelyUpdatedTable")
                        .withOld(sweepPriority().cellTsPairsExamined(10000).writeCount(50)
                                .lastSweepTimeMillis(ONE_MONTH_AGO)
                                .build())
                        .withNew(
                                sweepPriority()
                                        .lastSweepTimeMillis(
                                                ONE_MONTH_AGO)
                                        .build());

        given(rarelyUpdatedTable);

        whenCalculatingSweepPriorities();

        thenOnlyTablePrioritisedIs(rarelyUpdatedTable);
        thenTableHasZeroPriority(rarelyUpdatedTable);
    }

    @Test
    public void tableHasNotChangedMuch_butSweptLongAgo_hasPriority() {
        SweepPriorityHistory rarelyUpdatedTable =
                new SweepPriorityHistory("rarelyUpdatedTable")
                        .withOld(sweepPriority().cellTsPairsExamined(10000).writeCount(50)
                                .lastSweepTimeMillis(SEVEN_MONTHS_AGO)
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(SEVEN_MONTHS_AGO)
                                .build());

        given(rarelyUpdatedTable);

        whenCalculatingSweepPriorities();

        thenOnlyTablePrioritisedIs(rarelyUpdatedTable);
        thenTableHasPriority(rarelyUpdatedTable);
    }

    @Test
    public void ifWeDeletedManyValuesOnCassandra_andLessThanOneDayHasPassed_doNotSweep() {
        SweepPriorityHistory tableWithManyDeletes =
                new SweepPriorityHistory("tableWithManyDeletes")
                        .withOld(sweepPriority()
                                .lastSweepTimeMillis(ONE_MONTH_AGO)
                                .build())
                        .withNew(sweepPriority()
                                .staleValuesDeleted(1_500_000)
                                .lastSweepTimeMillis(TWELVE_HOURS_AGO)
                                .build());

        given(tableWithManyDeletes);

        whenCalculatingSweepPriorities();

        thenOnlyTablePrioritisedIs(tableWithManyDeletes);
        thenTableHasZeroPriority(tableWithManyDeletes);
    }

    @Test
    public void ifWeDeletedManyValuesOnCassandra_andMoreThanOneDayHasPassed_tableIsPrioritised() {
        SweepPriorityHistory tableWithManyDeletes =
                new SweepPriorityHistory("tableWithManyDeletes")
                        .withOld(sweepPriority()
                                .lastSweepTimeMillis(ONE_MONTH_AGO)
                                .build())
                        .withNew(sweepPriority()
                                .staleValuesDeleted(1_500_000)
                                .lastSweepTimeMillis(THIRTY_HOURS_AGO)
                                .build());

        given(tableWithManyDeletes);

        whenCalculatingSweepPriorities();

        thenOnlyTablePrioritisedIs(tableWithManyDeletes);
        thenTableHasPriority(tableWithManyDeletes);
    }

    @Test
    public void ifWeDeletedManyValuesNotOnCassandra_andLessThanOneDayHasPassed_tableIsPrioritised() {
        SweepPriorityHistory tableWithManyDeletes =
                new SweepPriorityHistory("tableWithManyDeletes")
                        .withOld(sweepPriority()
                                .lastSweepTimeMillis(ONE_MONTH_AGO)
                                .build())
                        .withNew(sweepPriority()
                                .staleValuesDeleted(1_500_000)
                                .lastSweepTimeMillis(TWELVE_HOURS_AGO)
                                .build());

        given(tableWithManyDeletes);
        givenNotCassandra();

        whenCalculatingSweepPriorities();

        thenOnlyTablePrioritisedIs(tableWithManyDeletes);
        thenTableHasPriority(tableWithManyDeletes);
    }

    @Test
    public void standardEstimatedTablePriorities() {
        SweepPriorityHistory tableWithLikelyManyValuesToSweep =
                new SweepPriorityHistory("tableWithLikelyManyValuesToSweep")
                        .withOld(sweepPriority()
                                .staleValuesDeleted(1_000_000)
                                .cellTsPairsExamined(10_000_000)
                                .writeCount(200_000)
                                .build())
                        .withNew(sweepPriority()
                                .writeCount(200_000)
                                .build());

        SweepPriorityHistory tableNotSweptInALongTime =
                new SweepPriorityHistory("tableNotSweptInALongTime")
                        .withOld(sweepPriority()
                                .staleValuesDeleted(10)
                                .cellTsPairsExamined(1_000_000)
                                .writeCount(20_000)
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(FIVE_DAYS_AGO)
                                .writeCount(200_000)
                                .build());

        SweepPriorityHistory recentlySweptTableWithFewWrites =
                new SweepPriorityHistory("recentlySweptTableWithFewWrites")
                        .withOld(sweepPriority()
                                .staleValuesDeleted(10)
                                .cellTsPairsExamined(1_000_000)
                                .writeCount(20_000)
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(TWELVE_HOURS_AGO)
                                .writeCount(20_000)
                                .build());

        given(tableWithLikelyManyValuesToSweep);
        given(tableNotSweptInALongTime);
        given(recentlySweptTableWithFewWrites);

        whenCalculatingSweepPriorities();

        thenNumberOfTablesIs(3);
        thenFirstTableHasHigherPriorityThanSecond(tableWithLikelyManyValuesToSweep, tableNotSweptInALongTime);
        thenFirstTableHasHigherPriorityThanSecond(tableNotSweptInALongTime, recentlySweptTableWithFewWrites);
    }

    @Test
    public void streamStore_valueTableHasZeroPriorityIfSweptRecently() {
        SweepPriorityHistory recentlySweptStreamStore =
                new SweepPriorityHistory(StreamTableType.VALUE.getTableName("recentlySweptStreamStore"))
                        .withOld(sweepPriority()
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(TWELVE_HOURS_AGO)
                                .build());
        SweepPriorityHistory notRecentlySweptStreamStore =
                new SweepPriorityHistory(StreamTableType.VALUE.getTableName("notRecentlySweptStreamStore"))
                        .withOld(sweepPriority()
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(FIVE_DAYS_AGO)
                                .build());

        given(notRecentlySweptStreamStore);
        given(recentlySweptStreamStore);

        whenCalculatingSweepPriorities();

        thenNumberOfTablesIs(2);
        thenTableHasZeroPriority(recentlySweptStreamStore);
        thenFirstTableHasHigherPriorityThanSecond(notRecentlySweptStreamStore, recentlySweptStreamStore);
    }

    @Test
    public void streamStore_valueTablePrioritisedByNumberOfWrites() {
        SweepPriorityHistory streamStoreValuesManyWrites =
                new SweepPriorityHistory(StreamTableType.VALUE.getTableName("streamStoreValuesManyWrites"))
                        .withOld(sweepPriority()
                                .writeCount(10)
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(FIVE_DAYS_AGO)
                                .writeCount(200)
                                .build());
        SweepPriorityHistory streamStoreValuesFewWrites =
                new SweepPriorityHistory(StreamTableType.VALUE.getTableName("streamStoreValuesFewWrites"))
                        .withOld(sweepPriority()
                                .writeCount(10)
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(FIVE_DAYS_AGO)
                                .writeCount(100)
                                .build());

        given(streamStoreValuesManyWrites);
        given(streamStoreValuesFewWrites);

        whenCalculatingSweepPriorities();

        thenNumberOfTablesIs(2);
        thenFirstTableHasHigherPriorityThanSecond(streamStoreValuesManyWrites, streamStoreValuesFewWrites);
    }

    @Test
    public void streamStore_valueTableHasHighestPriorityIfThresholdExceeded() {
        SweepPriorityHistory streamStoreValuesManyWrites =
                new SweepPriorityHistory(StreamTableType.VALUE.getTableName("streamStoreValuesManyWrites"))
                        .withOld(sweepPriority()
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(FIVE_DAYS_AGO)
                                .writeCount(SweepPriorityCalculator.STREAM_STORE_VALUES_TO_SWEEP + 10)
                                .build());

        given(streamStoreValuesManyWrites);

        whenCalculatingSweepPriorities();

        thenNumberOfTablesIs(1);
        thenHasHighestPriority(streamStoreValuesManyWrites);
    }

    @Test
    public void ifStreamStoreValueTableIsPriority_indexTableIsSweptFirst() {
        SweepPriorityHistory streamStoreValuesManyWrites =
                new SweepPriorityHistory(StreamTableType.VALUE.getTableName("streamStoreValuesManyWrites"))
                        .withOld(sweepPriority()
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(FIVE_DAYS_AGO)
                                .writeCount(SweepPriorityCalculator.STREAM_STORE_VALUES_TO_SWEEP + 10)
                                .build());
        SweepPriorityHistory streamStoreIndexManyWrites =
                new SweepPriorityHistory(StreamTableType.INDEX.getTableName("streamStoreValuesManyWrites"))
                        .withOld(sweepPriority()
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(SIX_DAYS_AGO)
                                .build());

        given(streamStoreValuesManyWrites);
        given(streamStoreIndexManyWrites);

        whenCalculatingSweepPriorities();

        thenNumberOfTablesIs(2);
        thenFirstTableHasHigherPriorityThanSecond(streamStoreIndexManyWrites, streamStoreValuesManyWrites);
    }

    @Test
    public void ifStreamStoreValueTableIsPriority_andIndexIsRecentlySwept_thenValuesTableIsHigherPriority() {
        SweepPriorityHistory streamStoreValuesManyWrites =
                new SweepPriorityHistory(StreamTableType.VALUE.getTableName("streamStoreValuesManyWrites"))
                        .withOld(sweepPriority()
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(SIX_DAYS_AGO)
                                .writeCount(SweepPriorityCalculator.STREAM_STORE_VALUES_TO_SWEEP + 10)
                                .build());
        SweepPriorityHistory streamStoreIndexManyWrites =
                new SweepPriorityHistory(StreamTableType.INDEX.getTableName("streamStoreValuesManyWrites"))
                        .withOld(sweepPriority()
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(FIVE_DAYS_AGO)
                                .build());

        given(streamStoreValuesManyWrites);
        given(streamStoreIndexManyWrites);

        whenCalculatingSweepPriorities();

        thenNumberOfTablesIs(2);
        thenFirstTableHasHigherPriorityThanSecond(streamStoreValuesManyWrites, streamStoreIndexManyWrites);
    }

    @Test
    public void doNotSweepStreamStoreValueTableWithinOneHourOfIndexTableBeingSwept() {
        SweepPriorityHistory streamStoreValuesManyWrites =
                new SweepPriorityHistory(StreamTableType.VALUE.getTableName("streamStoreValuesManyWrites"))
                        .withOld(sweepPriority()
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(SIX_DAYS_AGO)
                                .writeCount(SweepPriorityCalculator.STREAM_STORE_VALUES_TO_SWEEP + 10)
                                .build());
        SweepPriorityHistory streamStoreIndexManyWrites =
                new SweepPriorityHistory(StreamTableType.INDEX.getTableName("streamStoreValuesManyWrites"))
                        .withOld(sweepPriority()
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(THIRTY_MINUTES_AGO)
                                .build());

        given(streamStoreValuesManyWrites);
        given(streamStoreIndexManyWrites);

        whenCalculatingSweepPriorities();

        thenNumberOfTablesIs(2);
        thenTableHasZeroPriority(streamStoreValuesManyWrites);
        thenTableHasZeroPriority(streamStoreIndexManyWrites);
    }

    @Test
    public void streamStoreValueTableNotHighestPriority_indexNotSweptRecently_neitherExceedsHighestTablePriority1() {
        SweepPriorityHistory streamStoreValuesManyWrites =
                new SweepPriorityHistory(StreamTableType.VALUE.getTableName("streamStoreValuesManyWrites"))
                        .withOld(sweepPriority()
                                .writeCount(20)
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(SIX_DAYS_AGO)
                                .writeCount(200)
                                .build());
        SweepPriorityHistory streamStoreIndexManyWrites =
                new SweepPriorityHistory(StreamTableType.INDEX.getTableName("streamStoreValuesManyWrites"))
                        .withOld(sweepPriority()
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(FIVE_DAYS_AGO)
                                .build());
        SweepPriorityHistory highPriorityTable =
                new SweepPriorityHistory("highPriorityTable")
                        .withOld(sweepPriority()
                                .staleValuesDeleted(1_000_000)
                                .cellTsPairsExamined(10_000_000)
                                .writeCount(200_000)
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(ONE_MONTH_AGO)
                                .writeCount(200_000)
                                .build());

        given(streamStoreValuesManyWrites);
        given(streamStoreIndexManyWrites);
        given(highPriorityTable);

        whenCalculatingSweepPriorities();

        thenNumberOfTablesIs(3);
        thenFirstTableHasHigherPriorityThanSecond(streamStoreValuesManyWrites, streamStoreIndexManyWrites);
        thenFirstTableHasHigherPriorityThanSecond(highPriorityTable, streamStoreValuesManyWrites);
    }

    @Test
    public void streamStoreValueTableNotHighestPriority_indexNotSweptRecently_neitherExceedsHighestTablePriority2() {
        SweepPriorityHistory streamStoreValuesManyWrites =
                new SweepPriorityHistory(StreamTableType.VALUE.getTableName("streamStoreValuesManyWrites"))
                        .withOld(sweepPriority()
                                .writeCount(20)
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(FIVE_DAYS_AGO)
                                .writeCount(200)
                                .build());
        SweepPriorityHistory streamStoreIndexManyWrites =
                new SweepPriorityHistory(StreamTableType.INDEX.getTableName("streamStoreValuesManyWrites"))
                        .withOld(sweepPriority()
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(SIX_DAYS_AGO)
                                .build());
        SweepPriorityHistory highPriorityTable =
                new SweepPriorityHistory("highPriorityTable")
                        .withOld(sweepPriority()
                                .staleValuesDeleted(1_000_000)
                                .cellTsPairsExamined(10_000_000)
                                .writeCount(200_000)
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(ONE_MONTH_AGO)
                                .writeCount(200_000)
                                .build());

        given(streamStoreValuesManyWrites);
        given(streamStoreIndexManyWrites);
        given(highPriorityTable);

        whenCalculatingSweepPriorities();

        thenNumberOfTablesIs(3);
        thenFirstTableHasHigherPriorityThanSecond(streamStoreIndexManyWrites, streamStoreValuesManyWrites);
        thenFirstTableHasHigherPriorityThanSecond(highPriorityTable, streamStoreIndexManyWrites);
    }

    // Given
    private void givenNoTables() {
        // Nothing to do
    }

    private void given(TableReference... tableRefs) {
        allTables.addAll(Arrays.asList(tableRefs));
    }

    private void given(SweepPriorityHistory priority) {
        allTables.add(priority.tableRef);

        oldPriorities.add(priority.oldPriority);
        newPriorities.add(priority.newPriority);
    }

    private void givenNotCassandra() {
        isCassandra = false;
    }

    //When
    private void whenCalculatingSweepPriorities() {
        when(kvs.getAllTableNames()).thenReturn(allTables);
        when(sweepPriorityStore.loadOldPriorities(any(), anyLong())).thenReturn(oldPriorities);
        when(sweepPriorityStore.loadNewPriorities(any())).thenReturn(newPriorities);
        when(kvs.performanceIsSensitiveToTombstones()).thenReturn(isCassandra);

        priorities = calculator.calculateSweepPriorityScores(null, 0L);
    }

    //Then
    private void thenNoTablesToSweep() {
        Assert.assertThat(priorities.isEmpty(), is(true));
    }

    private void thenOnlyTablePrioritisedIs(TableReference table) {
        Assert.assertThat(priorities.size(), is(1));
        Assert.assertThat(priorities.containsKey(table), is(true));
    }

    private void thenOnlyTablePrioritisedIs(SweepPriorityHistory sweepPriorityHistory) {
        Assert.assertThat(priorities.size(), is(1));
        Assert.assertThat(priorities.containsKey(sweepPriorityHistory.tableRef), is(true));
    }

    private void thenTableHasPriority(TableReference table) {
        Assert.assertThat(priorities.get(table), greaterThan(0.0));
    }

    private void thenTableHasPriority(SweepPriorityHistory sweepPriorityHistory) {
        Assert.assertThat(priorities.get(sweepPriorityHistory.tableRef), greaterThan(0.0));
    }

    private void thenTableHasZeroPriority(SweepPriorityHistory sweepPriorityHistory) {
        Assert.assertThat(priorities.get(sweepPriorityHistory.tableRef), is(0.0));
    }

    private void thenNumberOfTablesIs(int expectedNumberOfTables) {
        Assert.assertThat(priorities.size(), is(expectedNumberOfTables));
    }

    private void thenFirstTableHasHigherPriorityThanSecond(SweepPriorityHistory higherPriorityTable,
            SweepPriorityHistory lowerPriorityTable) {
        double priority1 = priorities.get(higherPriorityTable.tableRef);
        double priority2 = priorities.get(lowerPriorityTable.tableRef);
        Assert.assertThat(priority1, greaterThan(priority2));
    }

    private void thenHasHighestPriority(SweepPriorityHistory highPriorityTable) {
        // Don't want to constrain implementation to use MAX_DOUBLE in case we do something more nuanced in the future.
        double priority = priorities.get(highPriorityTable.tableRef);
        Assert.assertThat(priority, greaterThan(1_000_000.0));
    }

    // helpers
    private static TableReference table(String name) {
        return TableReference.create(Namespace.create("test"), name);
    }

    private ImmutableSweepPriority.Builder sweepPriority() {
        return ImmutableSweepPriority.builder()
                .tableRef(table("placeholder"))
                .writeCount(1000)
                .lastSweepTimeMillis(TWO_HOURS_AGO)
                .minimumSweptTimestamp(100)
                .staleValuesDeleted(10)
                .cellTsPairsExamined(10000);
    }

    private class SweepPriorityHistory {
        final TableReference tableRef;
        ImmutableSweepPriority oldPriority;
        ImmutableSweepPriority newPriority;

        SweepPriorityHistory(String tableName) {
            this.tableRef = table(tableName);
        }

        SweepPriorityHistory withOld(ImmutableSweepPriority priority) {
            this.oldPriority = priority.withTableRef(tableRef);
            return this;
        }

        SweepPriorityHistory withNew(ImmutableSweepPriority priority) {
            this.newPriority = priority.withTableRef(tableRef);
            return this;
        }
    }
}
