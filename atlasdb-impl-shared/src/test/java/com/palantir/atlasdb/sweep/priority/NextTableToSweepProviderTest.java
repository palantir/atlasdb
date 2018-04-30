/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.priority;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class NextTableToSweepProviderTest {
    private NextTableToSweepProvider provider;

    private StreamStoreRemappingSweepPriorityCalculator calculator;
    private Map<TableReference, Double> priorities;
    private Set<String> priorityTables;
    private Set<String> blacklistTables;

    private Optional<TableReference> tableToSweep;

    @Before
    public void setup() {
        calculator = mock(StreamStoreRemappingSweepPriorityCalculator.class);
        priorities = new HashMap<>();
        priorityTables = new HashSet<>();
        blacklistTables = new HashSet<>();

        provider = new NextTableToSweepProvider(calculator);
    }

    @Test
    public void calculatorReturnsNoMappings_thenProviderReturnsEmpty() {
        givenNoPrioritiesReturned();

        whenGettingNextTableToSweep();

        thenProviderReturnsEmpty();
    }

    @Test
    public void calculatorReturnsOnlyZeroPriorities_thenProviderReturnsEmpty() {
        givenPriority(table("table1"), 0.0);
        givenPriority(table("table2"), 0.0);
        givenPriority(table("table3"), 0.0);

        whenGettingNextTableToSweep();

        thenProviderReturnsEmpty();
    }

    @Test
    public void calculatorReturnsSingleHighestPriority_thenProviderReturnsThatTable() {
        givenPriority(table("table1"), 0.0);
        givenPriority(table("table2"), 20.0);
        givenPriority(table("table3"), 30.0);
        givenPriority(table("table4"), 10.0);
        givenPriority(table("table5"), 0.0);

        whenGettingNextTableToSweep();

        thenTableChosenIs(table("table3"));
    }

    @Test
    public void calculatorReturnsManyTablesWithHighestPriority_thenProviderReturnsOneOfThose() {
        givenPriority(table("table1"), 0.0);
        givenPriority(table("table2"), 30.0);
        givenPriority(table("table3"), 30.0);
        givenPriority(table("table4"), 30.0);
        givenPriority(table("table5"), 10.0);

        whenGettingNextTableToSweep();

        Assert.assertTrue(tableToSweep.isPresent());
        Assert.assertThat(tableToSweep.get(), anyOf(is(table("table2")), is(table("table3")), is(table("table4"))));
    }

    @Test
    public void calculatorReturnsNonMaximumForPriorityTable_thenProviderStillSelectsIt() {
        givenPriority(table("table1"), 1.0);
        givenPriority(table("table2"), 10_000.0);
        givenPriorityOverride(table("table1"));

        whenGettingNextTableToSweep();

        thenTableChosenIs(table("table1"));
    }

    @Test
    public void calculatorReturnsZeroForPriorityTable_thenProviderStillSelectsIt() {
        givenPriority(table("table1"), 0.0);
        givenPriority(table("table2"), 1.0);
        givenPriorityOverride(table("table1"));

        whenGettingNextTableToSweep();

        thenTableChosenIs(table("table1"));
    }

    @Test
    public void calculatorReturnsLargeValueForBlacklistedTable_thenProviderStillDoesNotSelectIt() {
        givenPriority(table("table1"), 10_000.0);
        givenPriority(table("table2"), 1.0);
        givenBlacklisted(table("table1"));

        whenGettingNextTableToSweep();

        thenTableChosenIs(table("table2"));
    }

    @Test
    public void allTablesBlacklisted_thenProviderReturnsEmpty() {
        givenPriority(table("table1"), 1.0);
        givenPriority(table("table2"), 1.0);
        givenBlacklisted(table("table1"));
        givenBlacklisted(table("table2"));

        whenGettingNextTableToSweep();

        thenProviderReturnsEmpty();
    }

    private void givenNoPrioritiesReturned() {
        //Nothing to do
    }

    private void givenPriority(TableReference table, double priority) {
        priorities.put(table, priority);
    }

    private void givenPriorityOverride(TableReference table) {
        priorityTables.add(table.getQualifiedName());
    }

    private void givenBlacklisted(TableReference table) {
        blacklistTables.add(table.getQualifiedName());
    }

    private void whenGettingNextTableToSweep() {
        when(calculator.calculateSweepPriorityScores(any(), anyLong())).thenReturn(priorities);

        tableToSweep = provider.getNextTableToSweep(null, 0L, createOverrideConfig());
    }

    private SweepPriorityOverrideConfig createOverrideConfig() {
        return ImmutableSweepPriorityOverrideConfig.builder()
                .addAllPriorityTables(priorityTables)
                .addAllBlacklistTables(blacklistTables)
                .build();
    }

    private void thenProviderReturnsEmpty() {
        Assert.assertFalse(tableToSweep.isPresent());
    }

    private void thenTableChosenIs(TableReference table) {
        Assert.assertTrue(tableToSweep.isPresent());
        Assert.assertThat(tableToSweep.get(), is(table));
    }

    // helpers
    private static TableReference table(String name) {
        return TableReference.create(Namespace.create("test"), name);
    }
}
