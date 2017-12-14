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
package com.palantir.atlasdb.sweep.priority;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;

public class NextTableToSweepProvider {
    private final StreamStoreRemappingSweepPriorityCalculator calculator;

    @VisibleForTesting
    NextTableToSweepProvider(StreamStoreRemappingSweepPriorityCalculator streamStoreRemappingSweepPriorityCalculator) {
        this.calculator = streamStoreRemappingSweepPriorityCalculator;
    }

    public static NextTableToSweepProvider create(KeyValueService kvs, SweepPriorityStore sweepPriorityStore) {
        SweepPriorityCalculator basicCalculator = new SweepPriorityCalculator(kvs, sweepPriorityStore);
        StreamStoreRemappingSweepPriorityCalculator streamStoreRemappingSweepPriorityCalculator =
                new StreamStoreRemappingSweepPriorityCalculator(basicCalculator, sweepPriorityStore);

        return new NextTableToSweepProvider(streamStoreRemappingSweepPriorityCalculator);
    }

    public Optional<TableReference> getNextTableToSweep(Transaction tx, long conservativeSweepTimestamp) {
        Map<TableReference, Double> priorities = calculator.calculateSweepPriorities(tx, conservativeSweepTimestamp);

        Map<TableReference, Double> tablesWithNonZeroPriority = Maps.filterValues(priorities, notEqualTo(0.0));
        if (tablesWithNonZeroPriority.isEmpty()) {
            return Optional.empty();
        }

        List<TableReference> tablesWithHighestPriority = findTablesWithHighestPriority(tablesWithNonZeroPriority);

        Optional<TableReference> chosenTable = tablesWithHighestPriority.size() > 0
                ? Optional.of(getRandomValueFromList(tablesWithHighestPriority))
                : Optional.empty();

        // TODO (tboam): add logging

        return chosenTable;
    }

    private Predicate<Double> notEqualTo(Double target) {
        return Predicates.not(target::equals);
    }

    public static List<TableReference> findTablesWithHighestPriority(
            Map<TableReference, Double> tableToPriority) {
        Double maxPriority = Collections.max(tableToPriority.values());

        return tableToPriority.entrySet().stream()
                .filter(entry -> Objects.equals(entry.getValue(), maxPriority))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    private TableReference getRandomValueFromList(List<TableReference> tables) {
        return tables.get(ThreadLocalRandom.current().nextInt(tables.size()));
    }
}
