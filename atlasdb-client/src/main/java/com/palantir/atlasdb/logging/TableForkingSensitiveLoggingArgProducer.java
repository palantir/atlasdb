/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.logging;

import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Arg;
import java.util.List;
import java.util.function.Function;

public class TableForkingSensitiveLoggingArgProducer implements SensitiveLoggingArgProducer {
    private final ListMultimap<TableReference, SensitiveLoggingArgProducer> producers =
            MultimapBuilder.hashKeys().arrayListValues().build();
    private final SensitiveLoggingArgProducer catchall;

    public TableForkingSensitiveLoggingArgProducer(SensitiveLoggingArgProducer catchall) {
        this.catchall = catchall;
    }

    @Override
    public List<Arg<?>> getArgsForRow(TableReference tableReference, byte[] row) {
        return runOnRelevantProducersWithFallback(
                tableReference, producer -> producer.getArgsForRow(tableReference, row));
    }

    @Override
    public List<Arg<?>> getArgsForDynamicColumnsColumnKey(TableReference tableReference, byte[] row) {
        return runOnRelevantProducersWithFallback(
                tableReference, producer -> producer.getArgsForDynamicColumnsColumnKey(tableReference, row));
    }

    @Override
    public List<Arg<?>> getArgsForValue(TableReference tableReference, Cell cellReference, byte[] value) {
        return runOnRelevantProducersWithFallback(
                tableReference, producer -> producer.getArgsForValue(tableReference, cellReference, value));
    }

    public List<Arg<?>> runOnRelevantProducersWithFallback(
            TableReference tableReference, Function<SensitiveLoggingArgProducer, List<Arg<?>>> task) {
        List<SensitiveLoggingArgProducer> tableRelevantProducers = producers.get(tableReference);
        for (SensitiveLoggingArgProducer producer : tableRelevantProducers) {
            List<Arg<?>> producerResult = task.apply(producer);
            if (!producerResult.isEmpty()) {
                return producerResult;
            }
        }
        // Every relevant producer returned an empty list.
        return task.apply(catchall);
    }
}
