/*
 * Copyright 2016 Palantir Technologies
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

import org.eclipse.jetty.util.component.AbstractLifeCycle;
import org.eclipse.jetty.util.component.LifeCycle;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.remoting1.servers.jersey.HttpRemotingJerseyFeature;
import com.palantir.tritium.metrics.MetricRegistries;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class TimeLockServerLauncher extends Application<TimeLockServerConfiguration> {

    private final Optional<String> configurationFilePath;

    public TimeLockServerLauncher(String configurationFilePath) {
        this.configurationFilePath = Optional.of(configurationFilePath);
    }

    public TimeLockServerLauncher() {
        this.configurationFilePath = Optional.absent();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw new IllegalStateException("missing arguments, expected 2 got " + args.length);
        }

        String configurationFilePath = args[1];
        new TimeLockServerLauncher(configurationFilePath).run(args);
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
            Supplier<TimeLockServerConfiguration> timeLockServerConfigurationSupplier = createConfigurationSupplier(
                    configuration, environment);
            registerResources(environment, serverImpl, timeLockServerConfigurationSupplier);
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

    private Supplier<TimeLockServerConfiguration> createConfigurationSupplier(TimeLockServerConfiguration configuration,
            Environment environment) {
        return this.configurationFilePath.transform(filePath -> {
            Supplier<TimeLockServerConfiguration> configurationWatcher = new ConfigurationWatcher(configuration,
                    environment,
                    filePath);
            return configurationWatcher;
        }).or(Suppliers.ofInstance(configuration));
    }

    private static void registerResources(
            Environment environment,
            TimeLockServer serverImpl,
            Supplier<TimeLockServerConfiguration> timeLockServerConfigurationSupplier) {
        environment.jersey().register(HttpRemotingJerseyFeature.DEFAULT);
        environment.jersey().register(new TimeLockResource(() -> timeLockServerConfigurationSupplier.get().clients(),
                serverImpl::createInvalidatingTimeLockServices));
    }
}
