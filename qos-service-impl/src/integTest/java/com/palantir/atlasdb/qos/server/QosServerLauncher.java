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
package com.palantir.atlasdb.qos.server;

import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.atlasdb.qos.agent.QosAgent;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.remoting3.servers.jersey.HttpRemotingJerseyFeature;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class QosServerLauncher extends Application<QosServerConfig> {
    public static void main(String[] args) throws Exception {
        new QosServerLauncher().run(args);
    }

    @Override
    public void initialize(Bootstrap<QosServerConfig> bootstrap) {
        bootstrap.getObjectMapper().registerModule(new Jdk8Module());
        super.initialize(bootstrap);
    }

    @Override
    public void run(QosServerConfig configuration, Environment environment) {
        environment.jersey().register(HttpRemotingJerseyFeature.INSTANCE);
        MetricsManager metricsManager = MetricsManagers.of(environment.metrics(), new DefaultTaggedMetricRegistry());
        QosAgent agent = new QosAgent(metricsManager, configuration::runtime, configuration.install(),
                PTExecutors.newSingleThreadScheduledExecutor(), environment.jersey()::register);
        agent.createAndRegisterResources();
    }
}
