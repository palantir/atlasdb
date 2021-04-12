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
package com.palantir.atlasdb.timelock.benchmarks.server;

import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.atlasdb.timelock.config.CombinedTimeLockServerConfiguration;
import com.palantir.atlasdb.timelock.logging.NonBlockingFileAppenderFactory;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.timelock.paxos.TimeLockAgent;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.Optional;

public class TimelockBenchmarkServerLauncher extends Application<CombinedTimeLockServerConfiguration> {

    private static final UserAgent USER_AGENT =
            UserAgent.of(UserAgent.Agent.of("TimelockServerBenchmarkCluster", "0.0.0"));

    public static void main(String[] args) throws Exception {
        new TimelockBenchmarkServerLauncher().run(args);
    }

    @Override
    public void initialize(Bootstrap<CombinedTimeLockServerConfiguration> bootstrap) {
        bootstrap.getObjectMapper().registerModule(new Jdk8Module());
        bootstrap.getObjectMapper().registerSubtypes(NonBlockingFileAppenderFactory.class);
        super.initialize(bootstrap);
    }

    @Override
    public void run(CombinedTimeLockServerConfiguration configuration, Environment environment) throws Exception {
        TimeLockAgent agent = TimeLockAgent.create(
                MetricsManagers.of(environment.metrics(), SharedTaggedMetricRegistries.getSingleton()),
                configuration.install(),
                configuration::runtime, // this won't actually live reload
                USER_AGENT,
                CombinedTimeLockServerConfiguration.threadPoolSize(),
                CombinedTimeLockServerConfiguration.blockingTimeoutMs(),
                environment.jersey()::register,
                Optional.empty(),
                OrderableSlsVersion.valueOf("0.0.0"),
                ObjectMappers.newServerObjectMapper());
    }
}
