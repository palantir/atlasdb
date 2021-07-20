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

import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Arg;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Supports multiple {@link SensitiveLoggingArgProducer}s, that should be indexable by table reference. If multiple
 * {@link SensitiveLoggingArgProducer}s have been associated with a given {@link TableReference} and more than one
 * of them may definitively judge a context as safe or unsafe (not including the catchall producer), then behaviour
 * is non-deterministic. It is guaranteed that all {@link SensitiveLoggingArgProducer}s for a given
 * {@link TableReference} will be allowed to make a judgment before the catchall producer is invoked.
 */
public class TableForkingSensitiveLoggingArgProducer implements SensitiveLoggingArgProducer {
    // TODO (jkong): perf
    private final SetMultimap<TableReference, SensitiveLoggingArgProducer> producers =
            Multimaps.synchronizedSetMultimap(MultimapBuilder.hashKeys().hashSetValues().build());
    private final SensitiveLoggingArgProducer catchall;

    public TableForkingSensitiveLoggingArgProducer(SensitiveLoggingArgProducer catchall) {
        this.catchall = catchall;
    }

    public Optional<Arg<?>> runOnRelevantProducersWithFallback(
            TableReference tableReference, Function<SensitiveLoggingArgProducer, Optional<Arg<?>>> task) {
        Set<SensitiveLoggingArgProducer> tableRelevantProducers = producers.get(tableReference);
        for (SensitiveLoggingArgProducer producer : tableRelevantProducers) {
            Optional<Arg<?>> producerResult = task.apply(producer);
            if (producerResult.isPresent()) {
                return producerResult;
            }
        }
        // Every relevant producer returns empty
        return task.apply(catchall);
    }

    public void register(TableReference tableRef, SensitiveLoggingArgProducer sensitiveLoggingArgProducer) {
        producers.put(tableRef, sensitiveLoggingArgProducer);
    }

    @Override
    public Optional<Arg<?>> getArgForRow(
            TableReference tableReference, byte[] row, Function<byte[], Object> transform) {
        return runOnRelevantProducersWithFallback(
                tableReference, producer -> producer.getArgForRow(tableReference, row, transform));
    }

    @Override
    public Optional<Arg<?>> getArgForColumn(
            TableReference tableReference, byte[] row, Function<byte[], Object> transform) {
        return runOnRelevantProducersWithFallback(
                tableReference, producer -> producer.getArgForColumn(tableReference, row, transform));
    }

    @Override
    public Optional<Arg<?>> getNamedArgForValue(
            TableReference tableReference,
            Cell cellReference,
            byte[] value,
            Function<byte[], Object> transform,
            String name) {
        return runOnRelevantProducersWithFallback(
                tableReference,
                producer -> producer.getNamedArgForValue(tableReference, cellReference, value, transform, name));
    }
}
