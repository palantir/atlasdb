/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.benchmarks;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.atlasdb.timelock.benchmarks.config.TimelockBenchmarkClientConfig;
import com.palantir.atlasdb.timelock.logging.NonBlockingFileAppenderFactory;
import com.palantir.conjure.java.server.jersey.ConjureJerseyFeature;
import com.palantir.tritium.metrics.MetricRegistries;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class TimelockBenchmarkClientLauncher extends Application<TimelockBenchmarkClientConfig> {

    public static void main(String[] args) throws Exception {
        new TimelockBenchmarkClientLauncher().run(args);
    }

    @Override
    public void initialize(Bootstrap<TimelockBenchmarkClientConfig> bootstrap) {
        MetricRegistry metricRegistry = MetricRegistries.createWithHdrHistogramReservoirs();
        bootstrap.setMetricRegistry(metricRegistry);
        bootstrap.getObjectMapper().registerModule(new Jdk8Module());
        bootstrap.getObjectMapper().registerSubtypes(NonBlockingFileAppenderFactory.class);
        super.initialize(bootstrap);
    }

    @Override
    public void run(TimelockBenchmarkClientConfig configuration, Environment environment) throws Exception {
        environment.jersey().register(new BenchmarksResource(configuration.getAtlas()));
        environment.jersey().register(ConjureJerseyFeature.INSTANCE);
    }
}
