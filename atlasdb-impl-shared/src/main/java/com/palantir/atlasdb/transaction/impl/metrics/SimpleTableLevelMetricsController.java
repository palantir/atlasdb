/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl.metrics;

import com.codahale.metrics.Counter;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.util.MetricsManager;

public class SimpleTableLevelMetricsController implements TableLevelMetricsController {
    private final MetricsManager metricsManager;

    public SimpleTableLevelMetricsController(MetricsManager metricsManager) {
        this.metricsManager = metricsManager;
    }

    @Override
    public <T> Counter createAndRegisterCounter(Class<T> clazz, String metricName, TableReference tableReference) {
        return metricsManager.registerOrGetTaggedCounter(
                clazz, metricName, metricsManager.getTableNameTagFor(tableReference));
    }
}
