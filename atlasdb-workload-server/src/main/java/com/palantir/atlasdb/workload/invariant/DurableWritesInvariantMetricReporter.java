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

package com.palantir.atlasdb.workload.invariant;

import com.palantir.atlasdb.workload.DurableWritesMetrics;
import com.palantir.atlasdb.workload.store.TableAndWorkloadCell;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Map;
import java.util.function.Consumer;

public final class DurableWritesInvariantMetricReporter
        implements InvariantReporter<Map<TableAndWorkloadCell, MismatchedValue>> {

    private static final SafeLogger log = SafeLoggerFactory.get(DurableWritesInvariantMetricReporter.class);

    private final String workflow;
    private final DurableWritesMetrics durableWritesMetrics;

    public DurableWritesInvariantMetricReporter(String workflow, DurableWritesMetrics durableWritesMetrics) {
        this.workflow = workflow;
        this.durableWritesMetrics = durableWritesMetrics;
    }

    @Override
    public Invariant<Map<TableAndWorkloadCell, MismatchedValue>> invariant() {
        return DurableWritesInvariant.INSTANCE;
    }

    @Override
    public Consumer<Map<TableAndWorkloadCell, MismatchedValue>> consumer() {
        return mismatchingValues -> {
            mismatchingValues.forEach((tableAndWorkloadCell, _mismatchedValue) -> durableWritesMetrics
                    .numberOfViolations()
                    .workflow(workflow)
                    .table(tableAndWorkloadCell.tableName())
                    .build()
                    .inc());
            if (!mismatchingValues.isEmpty()) {
                log.warn(
                        "Durable writes invariant violations found: {}",
                        SafeArg.of("mismatchingValues", mismatchingValues));
            }
        };
    }
}
