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
import com.palantir.atlasdb.metrics.MetricPublicationFilter;

/**
 * Makes publication decisions for a given metric, as follows: for a given String identifier, filters out all but a
 * fixed number of the highest values. In the event of ties (e.g. a top-list of 10 where the 10th and 11th
 * highest values are equal), it is guaranteed that all greater values will be published, and furthermore some
 * of the tying values will be published so that the total number of published metrics is the size of the top-list
 * (though specifically which tying gauges are selected is nondeterministic).
 */
public interface MetricsFilterEvaluationContext {
    MetricPublicationFilter registerAndCreateTopNFilter(String key, Gauge<Long> value);
}
