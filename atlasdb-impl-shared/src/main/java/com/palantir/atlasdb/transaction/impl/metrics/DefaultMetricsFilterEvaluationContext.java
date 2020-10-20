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

import com.codahale.metrics.Gauge;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.metrics.MetricPublicationFilter;
import com.palantir.atlasdb.util.TopNMetricPublicationController;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class DefaultMetricsFilterEvaluationContext implements MetricsFilterEvaluationContext {
    private final Map<String, TopNMetricPublicationController<Long>> keyToPublicationController;
    private final Supplier<TopNMetricPublicationController<Long>> controllerFactory;

    @VisibleForTesting
    DefaultMetricsFilterEvaluationContext(Supplier<TopNMetricPublicationController<Long>> controllerFactory) {
        this.controllerFactory = controllerFactory;
        this.keyToPublicationController = new ConcurrentHashMap<>();
    }

    public static DefaultMetricsFilterEvaluationContext createDefault() {
        return create(AtlasDbConstants.DEFAULT_TABLES_TO_PUBLISH_TABLE_LEVEL_METRICS);
    }

    public static DefaultMetricsFilterEvaluationContext create(int toplistSize) {
        return new DefaultMetricsFilterEvaluationContext(() -> TopNMetricPublicationController.create(toplistSize));
    }

    @VisibleForTesting
    Set<String> getRegisteredKeys() {
        return keyToPublicationController.keySet();
    }

    @Override
    public MetricPublicationFilter registerAndCreateTopNFilter(String key, Gauge<Long> gauge) {
        return keyToPublicationController
                .computeIfAbsent(key, _name -> controllerFactory.get())
                .registerAndCreateFilter(gauge);
    }
}
