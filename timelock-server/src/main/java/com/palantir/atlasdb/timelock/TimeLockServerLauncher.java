/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock;

import java.util.UUID;
import java.util.function.Consumer;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.atlasdb.timelock.config.CombinedTimeLockServerConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockConfigMigrator;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.atlasdb.timelock.logging.NonBlockingFileAppenderFactory;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.remoting3.servers.jersey.HttpRemotingJerseyFeature;
import com.palantir.timelock.paxos.TimeLockAgent;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

/**
 * Provides a way of launching an embedded TimeLock server using Dropwizard. Should only be used in tests.
 */
public class TimeLockServerLauncher extends Application<TimeLockServerConfiguration> {
    public static void main(String[] args) throws Exception {
        new TimeLockServerLauncher().run(args);
    }

    @Override
    public void initialize(Bootstrap<TimeLockServerConfiguration> bootstrap) {
        MetricRegistry metricRegistry = SharedMetricRegistries
                .getOrCreate("AtlasDbTest" + UUID.randomUUID().toString());
        TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();
        AtlasDbMetrics.setMetricRegistries(metricRegistry, taggedMetricRegistry);
        bootstrap.setMetricRegistry(metricRegistry);
        bootstrap.getObjectMapper().registerSubtypes(NonBlockingFileAppenderFactory.class);
        bootstrap.getObjectMapper().registerModule(new Jdk8Module());
        super.initialize(bootstrap);
    }

    @Override
    public void run(TimeLockServerConfiguration configuration, Environment environment) {
        environment.getObjectMapper().registerModule(new Jdk8Module());
        environment.jersey().register(HttpRemotingJerseyFeature.INSTANCE);

        CombinedTimeLockServerConfiguration combined = TimeLockConfigMigrator.convert(configuration, environment);
        Consumer<Object> registrar = component -> environment.jersey().register(component);

        TimeLockAgent.create(
                combined.install(),
                combined::runtime, // this won't actually live reload
                combined.deprecated(),
                registrar);
    }
}
