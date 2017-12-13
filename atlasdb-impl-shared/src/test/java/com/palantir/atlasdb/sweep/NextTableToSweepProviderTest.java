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

package com.palantir.atlasdb.sweep;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.priority.ImmutableSweepPriority;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProviderImpl;
import com.palantir.atlasdb.sweep.priority.StreamStoreRemappingNextTableToSweepProviderImpl;
import com.palantir.atlasdb.sweep.priority.SweepPriority;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStore;

public class NextTableToSweepProviderTest {
    private KeyValueService kvs;
    private SweepPriorityStore sweepPriorityStore;

    private StreamStoreRemappingNextTableToSweepProviderImpl provider;

    private Set<TableReference> allTables;
    private List<SweepPriority> oldPriorities;
    private List<SweepPriority> newPriorities;
    private boolean isCassandra;

    private Map<TableReference, Double> priorities;

    @Before
    public void setup() {
        kvs = mock(KeyValueService.class);
        sweepPriorityStore = mock(SweepPriorityStore.class);

        NextTableToSweepProviderImpl nextTableToSweep = new NextTableToSweepProviderImpl(kvs, sweepPriorityStore);
        provider = new StreamStoreRemappingNextTableToSweepProviderImpl(nextTableToSweep, sweepPriorityStore);

        allTables = new HashSet<>(AtlasDbConstants.hiddenTables);
        oldPriorities = new ArrayList<>();
        newPriorities = new ArrayList<>();
        isCassandra = true;
    }


    @Test
    public void noTables() {
        givenNoTables();

        whenGettingTablesToSweep();

        thenNoTablesToSweep();
    }

    @Test
    public void unsweptTableIsAlwaysPrioritised() {
        TableReference unsweptTable = table("unswept1");

        given(unsweptTable);

        whenGettingTablesToSweep();

        thenOnlyTablePrioritisedIs(unsweptTable);
        thenTableHasPriority(unsweptTable);
    }

    @Test
    public void tableCleared_isLowPriority() {
        SweepPriorityHistory clearedTable = new SweepPriorityHistory("clearedTable")
                .withOld(sweepPriority().writeCount(100).build())
                .withNew(sweepPriority().writeCount(50).build());

        given(clearedTable);

        whenGettingTablesToSweep();

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

        whenGettingTablesToSweep();

        thenOnlyTablePrioritisedIs(unsweptTable);
    }

    @Test
    public void tableDidNotChangeMuchLastTimeWeSweptIt_doNotPrioritise() {
        SweepPriorityHistory rarelyUpdatedTable =
                new SweepPriorityHistory("rarelyUpdatedTable")
                        .withOld(sweepPriority().cellTsPairsExamined(10000).writeCount(50)
                                .lastSweepTimeMillis(DateTime.now().minusMonths(1).toDateTime().getMillis())
                                .build())
                        .withNew(
                                sweepPriority()
                                        .lastSweepTimeMillis(DateTime.now().minusMonths(1).toDateTime().getMillis())
                                        .build());

        given(rarelyUpdatedTable);

        whenGettingTablesToSweep();

        thenOnlyTablePrioritisedIs(rarelyUpdatedTable);
        thenTableHasZeroPriority(rarelyUpdatedTable);
    }

    @Test
    public void tableHasNotChangedMuch_butSweptLongAgo_hasPriority() {
        SweepPriorityHistory rarelyUpdatedTable =
                new SweepPriorityHistory("rarelyUpdatedTable")
                        .withOld(sweepPriority().cellTsPairsExamined(10000).writeCount(50)
                                .lastSweepTimeMillis(DateTime.now().minusMonths(7).toDateTime().getMillis())
                                .build())
                        .withNew(sweepPriority()
                                .lastSweepTimeMillis(DateTime.now().minusMonths(7).toDateTime().getMillis())
                                .build());

        given(rarelyUpdatedTable);

        whenGettingTablesToSweep();

        thenOnlyTablePrioritisedIs(rarelyUpdatedTable);
        thenTableHasPriority(rarelyUpdatedTable);
    }

    @Test
    public void ifWeDeletedManyValuesOnCassandra_andLessThanOneDayHasPassed_doNotSweep() {
        SweepPriorityHistory tableWithManyDeletes =
                new SweepPriorityHistory("tableWithManyDeletes")
                        .withOld(sweepPriority()
                                .lastSweepTimeMillis(DateTime.now().minusMonths(1).toDateTime().getMillis())
                                .build())
                        .withNew(sweepPriority()
                                .staleValuesDeleted(1_500_000)
                                .lastSweepTimeMillis(DateTime.now().minusHours(12).toDateTime().getMillis())
                                .build());

        given(tableWithManyDeletes);

        whenGettingTablesToSweep();

        thenOnlyTablePrioritisedIs(tableWithManyDeletes);
        thenTableHasZeroPriority(tableWithManyDeletes);
    }

    @Test
    public void ifWeDeletedManyValuesOnCassandra_andMoreThanOneDayHasPassed_tableIsPrioritised() {
        SweepPriorityHistory tableWithManyDeletes =
                new SweepPriorityHistory("tableWithManyDeletes")
                        .withOld(sweepPriority()
                                .lastSweepTimeMillis(DateTime.now().minusMonths(1).toDateTime().getMillis())
                                .build())
                        .withNew(sweepPriority()
                                .staleValuesDeleted(1_500_000)
                                .lastSweepTimeMillis(DateTime.now().minusHours(30).toDateTime().getMillis())
                                .build());

        given(tableWithManyDeletes);

        whenGettingTablesToSweep();

        thenOnlyTablePrioritisedIs(tableWithManyDeletes);
        thenTableHasPriority(tableWithManyDeletes);
    }

    @Test
    public void ifWeDeletedManyValuesNotOnCassandra_andLessThanOneDayHasPassed_tableIsPrioritised() {
        SweepPriorityHistory tableWithManyDeletes =
                new SweepPriorityHistory("tableWithManyDeletes")
                        .withOld(sweepPriority()
                                .lastSweepTimeMillis(DateTime.now().minusMonths(1).toDateTime().getMillis())
                                .build())
                        .withNew(sweepPriority()
                                .staleValuesDeleted(1_500_000)
                                .lastSweepTimeMillis(DateTime.now().minusHours(12).toDateTime().getMillis())
                                .build());

        given(tableWithManyDeletes);
        givenNotCassandra();

        whenGettingTablesToSweep();

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
                        .withNew( sweepPriority()
                                .lastSweepTimeMillis(DateTime.now().minusDays(5).toDateTime().getMillis())
                                .writeCount(20_0000)
                                .build());

        SweepPriorityHistory recentlySweptTableWithFewWrites =
                new SweepPriorityHistory("recentlySweptTableWithFewWrites")
                        .withOld(sweepPriority()
                                .staleValuesDeleted(10)
                                .cellTsPairsExamined(1_000_000)
                                .writeCount(20_000)
                                .build())
                        .withNew( sweepPriority()
                                .lastSweepTimeMillis(DateTime.now().minusHours(12).toDateTime().getMillis())
                                .writeCount(20_000)
                                .build());

        given(tableWithLikelyManyValuesToSweep);
        given(tableNotSweptInALongTime);
        given(recentlySweptTableWithFewWrites);

        whenGettingTablesToSweep();

        thenNumberOfTablesPrioritisedIs(3);
        thenFirstTableHasHigherPriorityThanSecond(tableWithLikelyManyValuesToSweep, tableNotSweptInALongTime);
        thenFirstTableHasHigherPriorityThanSecond(tableNotSweptInALongTime, recentlySweptTableWithFewWrites);
    }

    //    @Test
    //    public void notValueTableReturnsSameTable() {
    //        Map<TableReference, Double> singleNonStreamStoreTable = ImmutableMap.of(NOT_SS_VALUE_TABLE, 1.0);
    //        when(delegate.computeSweepPriorities(any(), anyLong())).thenReturn(singleNonStreamStoreTable);
    //
    //        Optional<TableReference> returnedTable = provider.computeSweepPriorities(mockedTransaction, 1L);
    //        assertThat(returnedTable).isEqualTo(selectedTable);
    //    }
    //
    //    @Test
    //    public void valueTableReturnsIndexThenValueTables() {
    //        Optional<TableReference> selectedTable = Optional.of(SS_VALUE_TABLE);
    //        when(delegate.computeSweepPriorities(any(), anyLong())).thenReturn(selectedTable);
    //
    //        assertReturnsIndexThenValueTable();
    //    }
    //
    //    @Test
    //    @SuppressWarnings("unchecked")
    //    public void notValueTableAfterValueTableIsReturnedCorrectly() {
    //        Optional<TableReference> selectedTable = Optional.of(SS_VALUE_TABLE);
    //        Optional<TableReference> nextSelectedTable = Optional.of(NOT_SS_VALUE_TABLE);
    //        when(delegate.computeSweepPriorities(any(), anyLong())).thenReturn(selectedTable, nextSelectedTable);
    //
    //        assertReturnsIndexThenValueTable();
    //
    //        Optional<TableReference> followupReturnedTable = provider.computeSweepPriorities(mockedTransaction, 1L);
    //        assertThat(followupReturnedTable).isEqualTo(Optional.of(NOT_SS_VALUE_TABLE));
    //    }
    //
    //    private void assertReturnsIndexThenValueTable() {
    //        Optional<TableReference> returnedTable = provider.computeSweepPriorities(mockedTransaction, 1L);
    //        assertThat(returnedTable).isEqualTo(Optional.of(SS_INDEX_TABLE));
    //
    //        Optional<TableReference> followupReturnedTable = provider.computeSweepPriorities(mockedTransaction, 1L);
    //        assertThat(followupReturnedTable).isEqualTo(Optional.of(SS_VALUE_TABLE));
    //    }

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

    private void whenGettingTablesToSweep() {
        when(kvs.getAllTableNames()).thenReturn(allTables);
        when(sweepPriorityStore.loadOldPriorities(any(), anyLong())).thenReturn(oldPriorities);
        when(sweepPriorityStore.loadNewPriorities(any())).thenReturn(newPriorities);
        when(kvs.performanceIsSensitiveToTombstones()).thenReturn(isCassandra);

        priorities = provider.computeSweepPriorities(null, 0L);
    }

    private void thenNoTablesToSweep() {
        Assert.assertThat(priorities.isEmpty(), CoreMatchers.is(true));
    }

    private void thenOnlyTablePrioritisedIs(TableReference table) {
        Assert.assertThat(priorities.size(), CoreMatchers.is(1));
        Assert.assertThat(priorities.containsKey(table), CoreMatchers.is(true));
    }

    private void thenOnlyTablePrioritisedIs(SweepPriorityHistory sweepPriorityHistory) {
        Assert.assertThat(priorities.size(), CoreMatchers.is(1));
        Assert.assertThat(priorities.containsKey(sweepPriorityHistory.tableRef), CoreMatchers.is(true));
    }

    private void thenTableHasPriority(TableReference table) {
        Assert.assertThat(priorities.get(table), Matchers.greaterThan(0.0));
    }

    private void thenTableHasPriority(SweepPriorityHistory sweepPriorityHistory) {
        Assert.assertThat(priorities.get(sweepPriorityHistory.tableRef), Matchers.greaterThan(0.0));
    }

    private void thenTableHasZeroPriority(SweepPriorityHistory sweepPriorityHistory) {
        Assert.assertThat(priorities.get(sweepPriorityHistory.tableRef), CoreMatchers.is(0.0));
    }

    private void thenNumberOfTablesPrioritisedIs(int expectedNumberOfTables) {
        Assert.assertThat(priorities.size(), CoreMatchers.is(expectedNumberOfTables));
    }

    private void thenFirstTableHasHigherPriorityThanSecond(SweepPriorityHistory higherPriorityTable,
            SweepPriorityHistory lowerPriorityTable) {
        double priority1 = priorities.get(higherPriorityTable.tableRef);
        double priority2 = priorities.get(lowerPriorityTable.tableRef);
        Assert.assertThat(priority1, Matchers.greaterThan(priority2));
    }

    // helpers
    private static TableReference table(String name) {
        return TableReference.create(Namespace.create("test"), name);
    }

    private ImmutableSweepPriority.Builder sweepPriority() {
        return ImmutableSweepPriority.builder()
                .tableRef(table("placeholder"))
                .writeCount(1000)
                .lastSweepTimeMillis(DateTime.now().getMillis())
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
