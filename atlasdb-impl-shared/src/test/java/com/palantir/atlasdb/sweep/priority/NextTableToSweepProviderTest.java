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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.sweep.TableToSweep;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockService;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;

public class NextTableToSweepProviderTest {
    private NextTableToSweepProvider provider;

    private LockService lockService;
    private StreamStoreRemappingSweepPriorityCalculator calculator;
    private Map<TableReference, Double> priorities;
    private Set<String> priorityTables;
    private Set<String> denylistTables;

    private List<Optional<TableToSweep>> tablesToSweep = new ArrayList<>();

    @Before
    public void setup() throws InterruptedException {
        lockService = mock(LockService.class);
        LockRefreshToken token = new LockRefreshToken(BigInteger.ONE, Long.MAX_VALUE);
        when(lockService.lock(anyString(), any())).thenReturn(token);

        calculator = mock(StreamStoreRemappingSweepPriorityCalculator.class);
        priorities = new HashMap<>();
        priorityTables = new HashSet<>();
        denylistTables = new HashSet<>();

        provider = new NextTableToSweepProvider(lockService, calculator);
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
    public void calculatorReturnsMultiplePriorities_highestPriorityIsLocked_thenProviderReturnsSecondHighest()
            throws InterruptedException {
        givenPriority(table("table1"), 30.0);
        givenPriority(table("table2"), 20.0);
        givenTableIsLocked("table1");

        whenGettingNextTableToSweep();

        thenTableChosenIs(table("table2"));
    }

    @Test
    public void calculatorReturnsMultiplePriorities_nonZeroPrioritiesLocked_thenProviderReturnsNothing()
            throws InterruptedException {
        givenPriority(table("table1"), 30.0);
        givenPriority(table("table2"), 20.0);
        givenPriority(table("table3"), 0.0);
        givenTableIsLocked("table1");
        givenTableIsLocked("table2");

        whenGettingNextTableToSweep();

        thenProviderReturnsEmpty();
    }

    @Test
    public void calculatorReturnsManyTablesWithHighestPriority_thenProviderReturnsOneOfThose() {
        givenPriority(table("table1"), 0.0);
        givenPriority(table("table2"), 30.0);
        givenPriority(table("table3"), 30.0);
        givenPriority(table("table4"), 30.0);
        givenPriority(table("table5"), 10.0);

        whenGettingNextTableToSweep();

        Optional<TableToSweep> tableToSweep = Iterables.getOnlyElement(tablesToSweep);
        assertThat(tableToSweep).isPresent();
        assertThat(ImmutableSet.of(table("table2"), table("table3"), table("table4")))
                .contains(tableToSweep.get().getTableRef());
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
    public void priorityTablesAreLocked_thenProviderSelectsOtherTable() throws InterruptedException {
        givenPriority(table("table1"), 20.0);
        givenPriority(table("table2"), 10.0);
        givenPriorityOverride(table("table1"));
        givenTableIsLocked("table1");

        whenGettingNextTableToSweep();

        thenProviderReturnsEmpty();
    }

    @Test
    public void calculatorReturnsLargeValueForDenylistedTable_thenProviderStillDoesNotSelectIt() {
        givenPriority(table("table1"), 10_000.0);
        givenPriority(table("table2"), 1.0);
        givenDenylisted(table("table1"));

        whenGettingNextTableToSweep();

        thenTableChosenIs(table("table2"));
    }

    @Test
    public void allTablesDenylisted_thenProviderReturnsEmpty() {
        givenPriority(table("table1"), 1.0);
        givenPriority(table("table2"), 1.0);
        givenDenylisted(table("table1"));
        givenDenylisted(table("table2"));

        whenGettingNextTableToSweep();

        thenProviderReturnsEmpty();
    }

    @Test
    public void calculatorReturnsMultipleTablesWithPriorityOverride_thenProviderDoesNotPinAnyOne() {
        givenPriority(table("table1"), 1.0);
        givenPriority(table("table2"), 1.0);
        givenPriority(table("table3"), 1.0);
        givenPriorityOverride(table("table1"));
        givenPriorityOverride(table("table2"));
        givenPriorityOverride(table("table3"));

        whenGettingNextTableToSweepMultipleTimes(1_000);

        // There is a chance this test strobes, but assuming uniform randomness this is bounded above by
        // 3 * (2/3)^1000 or ~2.4e-176.
        thenTableIsChosenAtLeastOnce(table("table1"));
        thenTableIsChosenAtLeastOnce(table("table2"));
        thenTableIsChosenAtLeastOnce(table("table3"));
    }

    private void givenNoPrioritiesReturned() {
        // Nothing to do
    }

    private void givenPriority(TableReference table, double priority) {
        priorities.put(table, priority);
    }

    private void givenPriorityOverride(TableReference table) {
        priorityTables.add(table.getQualifiedName());
    }

    private void givenDenylisted(TableReference table) {
        denylistTables.add(table.getQualifiedName());
    }

    private void givenTableIsLocked(String table) throws InterruptedException {
        when(lockService.lock(any(), requestContaining(table))).thenReturn(null);
    }

    private LockRequest requestContaining(String table) {
        return argThat(argument -> argument != null
                && argument.getLockDescriptors().stream()
                        .anyMatch(descriptor -> descriptor.getLockIdAsString().contains(table)));
    }

    private void whenGettingNextTableToSweep() {
        when(calculator.calculateSweepPriorityScores(any(), anyLong())).thenReturn(priorities);

        tablesToSweep.add(provider.getNextTableToSweep(null, 0L, createOverrideConfig()));
    }

    private void whenGettingNextTableToSweepMultipleTimes(int times) {
        IntStream.range(0, times).forEach(unused -> whenGettingNextTableToSweep());
    }

    private SweepPriorityOverrideConfig createOverrideConfig() {
        return ImmutableSweepPriorityOverrideConfig.builder()
                .addAllPriorityTables(priorityTables)
                .addAllBlacklistTables(denylistTables)
                .build();
    }

    private void thenProviderReturnsEmpty() {
        Optional<TableToSweep> tableToSweep = Iterables.getOnlyElement(tablesToSweep);
        assertThat(tableToSweep)
                .describedAs("expected to not have chosen a table!")
                .isNotPresent();
    }

    private void thenTableChosenIs(TableReference table) {
        Optional<TableToSweep> tableToSweep = Iterables.getOnlyElement(tablesToSweep);
        assertThat(tableToSweep).describedAs("expected to have chosen a table!").isPresent();
        assertThat(tableToSweep.get().getTableRef()).isEqualTo(table);
    }

    private void thenTableIsChosenAtLeastOnce(TableReference table) {
        assertThat(tablesToSweep.stream()
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .map(TableToSweep::getTableRef)
                        .anyMatch(chosenTable -> chosenTable.equals(table)))
                .describedAs("expected table " + table + " to be chosen at least once, but wasn't!")
                .isTrue();
    }

    // helpers
    private static TableReference table(String name) {
        return TableReference.create(Namespace.create("test"), name);
    }
}
