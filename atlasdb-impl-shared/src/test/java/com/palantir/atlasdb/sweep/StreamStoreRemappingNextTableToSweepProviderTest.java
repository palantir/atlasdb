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

import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.stream.StreamTableType;
import com.palantir.atlasdb.sweep.priority.ImmutableSweepPriority;
import com.palantir.atlasdb.sweep.priority.NextTableToSweepProviderImpl;
import com.palantir.atlasdb.sweep.priority.StreamStoreRemappingNextTableToSweepProviderImpl;
import com.palantir.atlasdb.sweep.priority.SweepPriority;
import com.palantir.atlasdb.sweep.priority.SweepPriorityStore;
import com.palantir.util.Pair;

public class StreamStoreRemappingNextTableToSweepProviderTest {
    private static final String TEST_TABLE = "test";
    private static final String SS_VALUE_TABLE_NAME = StreamTableType.VALUE.getTableName(TEST_TABLE);
    private static final String SS_INDEX_TABLE_NAME = StreamTableType.INDEX.getTableName(TEST_TABLE);

    private static final TableReference NOT_SS_VALUE_TABLE = TableReference.createWithEmptyNamespace("test");
    private static final TableReference SS_VALUE_TABLE = TableReference.createWithEmptyNamespace(SS_VALUE_TABLE_NAME);
    private static final TableReference SS_INDEX_TABLE = TableReference.createWithEmptyNamespace(SS_INDEX_TABLE_NAME);

    private KeyValueService kvs;
    private SweepPriorityStore sweepPriorityStore;

    private StreamStoreRemappingNextTableToSweepProviderImpl provider;

    private Set<TableReference> allTables;
    private List<SweepPriority> oldPriorities;
    private List<SweepPriority> newPriorities;

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
        Pair<SweepPriority, SweepPriority> clearedTable = Pair.create(
                sweepPriority("clearedTable").writeCount(100).build(),
                sweepPriority("clearedTable").writeCount(50).build());

        given(clearedTable);

        whenGettingTablesToSweep();

        thenOnlyTablePrioritisedIs(clearedTable.getLhSide().tableRef());
        thenTableHasZeroPriority(clearedTable.getLhSide().tableRef());
    }

    @Test
    public void unsweptTableAndNormalTable_prioritiseUnsweptTable() {
        TableReference unsweptTable = table("unswept1");

        Pair<SweepPriority, SweepPriority> normalTable = Pair.create(
                sweepPriority("normalTable").writeCount(50).build(),
                sweepPriority("normalTable").writeCount(100).build());

        given(unsweptTable);
        given(normalTable);

        whenGettingTablesToSweep();

        thenOnlyTablePrioritisedIs(unsweptTable);
    }

    @Test
    public void tableDidNotChangeMuchLastTimeWeSweptIt_doNotPrioritise() {
        Pair<SweepPriority, SweepPriority> rarelyUpdatedTable = Pair.create(
                sweepPriority("rarelyUpdatedTable").cellTsPairsExamined(10000).writeCount(50)
                        .lastSweepTimeMillis(DateTime.now().minusMonths(1).toDateTime().getMillis())
                        .build(),
                sweepPriority("rarelyUpdatedTable")
                        .lastSweepTimeMillis(DateTime.now().minusMonths(1).toDateTime().getMillis())
                        .build());

        given(rarelyUpdatedTable);

        whenGettingTablesToSweep();

        thenOnlyTablePrioritisedIs(rarelyUpdatedTable.getLhSide().tableRef());
        thenTableHasZeroPriority(rarelyUpdatedTable.getLhSide().tableRef());
    }

    @Test
    public void tableHasNotChangedMuch_butSweptLongAgo_hasPriority() {
        Pair<SweepPriority, SweepPriority> rarelyUpdatedTable = Pair.create(
                sweepPriority("rarelyUpdatedTable").cellTsPairsExamined(10000).writeCount(50)
                        .lastSweepTimeMillis(DateTime.now().minusMonths(7).toDateTime().getMillis())
                        .build(),
                sweepPriority("rarelyUpdatedTable")
                        .lastSweepTimeMillis(DateTime.now().minusMonths(7).toDateTime().getMillis())
                        .build());

        given(rarelyUpdatedTable);

        whenGettingTablesToSweep();

        thenOnlyTablePrioritisedIs(rarelyUpdatedTable.getLhSide().tableRef());
        thenTableHasPriority(rarelyUpdatedTable.getLhSide().tableRef());
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

    private void given(Pair<SweepPriority, SweepPriority> priorities) {
        allTables.add(priorities.getLhSide().tableRef());

        oldPriorities.add(priorities.getLhSide());
        newPriorities.add(priorities.getRhSide());
    }

    private void whenGettingTablesToSweep() {
        when(kvs.getAllTableNames()).thenReturn(allTables);
        when(sweepPriorityStore.loadOldPriorities(any(), anyLong())).thenReturn(oldPriorities);
        when(sweepPriorityStore.loadNewPriorities(any())).thenReturn(newPriorities);

        priorities = provider.computeSweepPriorities(null, 0L);
    }

    private void thenNoTablesToSweep() {
        Assert.assertThat(priorities.isEmpty(), CoreMatchers.is(true));
    }

    private void thenOnlyTablePrioritisedIs(TableReference table) {
        Assert.assertThat(priorities.size(), CoreMatchers.is(1));
        Assert.assertThat(priorities.containsKey(table), CoreMatchers.is(true));
    }

    private void thenTableHasPriority(TableReference table) {
        Assert.assertThat(priorities.get(table), Matchers.greaterThan(0.0));
    }

    private void thenTableHasZeroPriority(TableReference table) {
        Assert.assertThat(priorities.get(table), CoreMatchers.is(0.0));
    }

    // helpers
    private static final TableReference table(String name) {
        return TableReference.create(Namespace.create("test"), name);
    }

    private ImmutableSweepPriority.Builder sweepPriority(String tableName) {
        return ImmutableSweepPriority.builder()
                .tableRef(table(tableName))
                .writeCount(100)
                .lastSweepTimeMillis(DateTime.now().getMillis())
                .minimumSweptTimestamp(100)
                .staleValuesDeleted(100)
                .cellTsPairsExamined(100);
    }
}
