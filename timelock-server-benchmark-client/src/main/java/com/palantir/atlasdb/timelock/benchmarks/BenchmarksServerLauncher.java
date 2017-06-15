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

package com.palantir.atlasdb.timelock.benchmarks;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.atlasdb.timelock.benchmarks.config.TimelockBenchmarksConfig;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.tritium.metrics.MetricRegistries;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class BenchmarksServerLauncher extends Application<TimelockBenchmarksConfig> {

    public static void main(String[] args) throws Exception {
        new BenchmarksServerLauncher().run(args);
    }

    @Override
    public void initialize(Bootstrap<TimelockBenchmarksConfig> bootstrap) {
        MetricRegistry metricRegistry = MetricRegistries.createWithHdrHistogramReservoirs();
        AtlasDbMetrics.setMetricRegistry(metricRegistry);
        bootstrap.setMetricRegistry(metricRegistry);
        bootstrap.getObjectMapper().registerModule(new Jdk8Module());
        super.initialize(bootstrap);
    }

    @Override
    public void run(TimelockBenchmarksConfig configuration, Environment environment) throws Exception {
        environment.jersey().register(new BenchmarksResource(configuration.getAtlas()));
    }
}
