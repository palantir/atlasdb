/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.util;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.workload.store.WorkloadCell;
import com.palantir.atlasdb.workload.transaction.witnessed.WitnessedTransactionAction;
import com.palantir.atlasdb.workload.transaction.witnessed.range.WitnessedRowsColumnRangeReadTransactionAction;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.function.Consumer;

public final class RangeIterators {
    private RangeIterators() {
        // Utility class
    }

    public static WitnessingIterator<Entry<WorkloadCell, Integer>> rowColumnRangeIterator(
            RowColumnRangeReadIterationContext context,
            Iterator<Entry<Cell, byte[]>> rawIterator,
            Consumer<WitnessedTransactionAction> eventSink) {
        Iterator<Entry<WorkloadCell, Integer>> rawWorkloadValueIterator =
                Iterators.transform(rawIterator, RangeIterators::toWorkloadCellAndValue);
        return new WitnessingIterator<>(
                context.iteratorIdentifier(),
                rawWorkloadValueIterator,
                (uuid, entry) -> RangeIterators.addReadWitness(context, entry, eventSink),
                uuid -> RangeIterators.addExhaustionWitness(context, eventSink));
    }

    private static void addReadWitness(
            RowColumnRangeReadIterationContext context,
            Entry<WorkloadCell, Integer> next,
            Consumer<WitnessedTransactionAction> eventSink) {
        eventSink.accept(WitnessedRowsColumnRangeReadTransactionAction.builder()
                .iteratorIdentifier(context.iteratorIdentifier())
                .specificRow(context.specificRow())
                .table(context.table())
                .cell(next.getKey())
                .value(next.getValue())
                .build());
    }

    private static void addExhaustionWitness(
            RowColumnRangeReadIterationContext context, Consumer<WitnessedTransactionAction> eventSink) {
        eventSink.accept(WitnessedRowsColumnRangeReadTransactionAction.builder()
                .iteratorIdentifier(context.iteratorIdentifier())
                .specificRow(context.specificRow())
                .table(context.table())
                .build());
    }

    private static Entry<WorkloadCell, Integer> toWorkloadCellAndValue(Entry<Cell, byte[]> rawEntry) {
        return Maps.immutableEntry(
                AtlasDbUtils.toWorkloadCell(rawEntry.getKey()), AtlasDbUtils.toWorkloadValue(rawEntry.getValue()));
    }
}
