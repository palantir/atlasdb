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

import java.util.Map;

import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.remoting2.servers.jersey.HttpRemotingJerseyFeature;
import com.palantir.tritium.metrics.MetricRegistries;
import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class TimeLockServerLauncher extends Application<TimeLockServerConfiguration> {
    public static void main(String[] args) throws Exception {
        new TimeLockServerLauncher().run(args);
    }

    @Override
    public void initialize(Bootstrap<TimeLockServerConfiguration> bootstrap) {
        MetricRegistry metricRegistry = MetricRegistries.createWithHdrHistogramReservoirs();
        AtlasDbMetrics.setMetricRegistry(metricRegistry);
        bootstrap.setMetricRegistry(metricRegistry);
        super.initialize(bootstrap);
    }

    @Override
    public void run(TimeLockServerConfiguration configuration, Environment environment) {
        TimeLockServer serverImpl = configuration.algorithm().createServerImpl(environment);
        try {
            serverImpl.onStartup(configuration);
            registerResources(configuration, environment, serverImpl);
        } catch (Exception e) {
            serverImpl.onStartupFailure();
            throw e;
        }

        environment.lifecycle().addLifeCycleListener(new AbstractLifeCycle.AbstractLifeCycleListener() {
            @Override
            public void lifeCycleStopped(LifeCycle event) {
                serverImpl.onStop();
            }
        });
    }

    private static void registerResources(
            TimeLockServerConfiguration configuration,
            Environment environment,
            TimeLockServer serverImpl) {
        Map<String, TimeLockServices> clientToServices = ImmutableMap.copyOf(Maps.asMap(
                configuration.clients(),
                client -> serverImpl.createInvalidatingTimeLockServices(client,
                        configuration.slowLockLogTriggerMillis())));

        environment.getObjectMapper().registerModule(new Jdk8Module());
        environment.jersey().register(HttpRemotingJerseyFeature.DEFAULT);
        environment.jersey().register(new TimeLockResource(clientToServices));
    }

}
