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

import java.util.function.Consumer;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.atlasdb.timelock.clock.ClockServiceImpl;
import com.palantir.atlasdb.timelock.config.CombinedTimeLockServerConfiguration;
import com.palantir.atlasdb.timelock.config.TimeLockConfigMigrator;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.atlasdb.timelock.logging.NonBlockingFileAppenderFactory;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.remoting2.servers.jersey.HttpRemotingJerseyFeature;
import com.palantir.timelock.TimeLockAgent;
import com.palantir.tritium.metrics.MetricRegistries;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.reactivex.Observable;

public class TimeLockServerLauncher extends Application<TimeLockServerConfiguration> {
    public static void main(String[] args) throws Exception {
        new TimeLockServerLauncher().run(args);
    }

    @Override
    public void initialize(Bootstrap<TimeLockServerConfiguration> bootstrap) {
        MetricRegistry metricRegistry = MetricRegistries.createWithHdrHistogramReservoirs();
        AtlasDbMetrics.setMetricRegistry(metricRegistry);
        bootstrap.setMetricRegistry(metricRegistry);
        bootstrap.getObjectMapper().registerSubtypes(NonBlockingFileAppenderFactory.class);
        super.initialize(bootstrap);
    }

    @Override
    public void run(TimeLockServerConfiguration configuration, Environment environment) {
        environment.getObjectMapper().registerModule(new Jdk8Module());
        environment.jersey().register(HttpRemotingJerseyFeature.DEFAULT);

        CombinedTimeLockServerConfiguration combined = TimeLockConfigMigrator.convert(configuration, environment);
        Consumer<Object> registrar = component -> environment.jersey().register(component);
        TimeLockAgent agent = combined.install().algorithm().createTimeLockAgent(
                combined.install(),
                Observable.just(combined.runtime()), // this won't actually live reload
                combined.deprecated(),
                registrar);
        agent.createAndRegisterResources();

        environment.jersey().register(new ClockServiceImpl());
    }
}
