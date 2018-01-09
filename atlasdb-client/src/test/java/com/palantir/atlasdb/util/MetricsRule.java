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
package com.palantir.atlasdb.util;

import org.junit.rules.ExternalResource;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

public class MetricsRule extends ExternalResource {

    @Override
    protected void before() throws Throwable {
        super.before();
        AtlasDbMetrics.setMetricRegistries(SharedMetricRegistries.getOrCreate("AtlasDbTest"),
                new DefaultTaggedMetricRegistry());
    }

    @Override
    protected void after() {
        super.after();
        MetricRegistry metrics = metrics();
        if (metrics != null) {
            ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).build();
            reporter.report();
            reporter.close();
            SharedMetricRegistries.remove("AtlasDbTest");
        }
    }

    public MetricRegistry metrics() {
        return AtlasDbMetrics.getMetricRegistry();
    }

}
