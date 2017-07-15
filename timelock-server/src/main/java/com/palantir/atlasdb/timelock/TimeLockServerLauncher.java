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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.palantir.atlasdb.timelock.config.CombinedTimeLockServerConfiguration;
import com.palantir.atlasdb.timelock.config.DropwizardLiveReloader;
import com.palantir.atlasdb.timelock.config.TimeLockConfigMigrator;
import com.palantir.atlasdb.timelock.config.TimeLockServerConfiguration;
import com.palantir.atlasdb.util.AtlasDbMetrics;
import com.palantir.remoting2.servers.jersey.HttpRemotingJerseyFeature;
import com.palantir.timelock.TimeLockAgent;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.tritium.metrics.MetricRegistries;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.schedulers.Schedulers;

public class TimeLockServerLauncher extends Application<TimeLockServerConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(TimeLockServerLauncher.class);

    // TODO (jkong): There's got to be a better way
    private static Path configurationPath;
    private DropwizardLiveReloader liveReloader;

    public static void main(String[] args) throws Exception {
        configurationPath = Paths.get(args[1]);
        new TimeLockServerLauncher().run(args);
    }

    @Override
    public void initialize(Bootstrap<TimeLockServerConfiguration> bootstrap) {
        liveReloader = DropwizardLiveReloader.create(bootstrap, configurationPath.toFile());

        MetricRegistry metricRegistry = MetricRegistries.createWithHdrHistogramReservoirs();
        AtlasDbMetrics.setMetricRegistry(metricRegistry);
        bootstrap.setMetricRegistry(metricRegistry);
        super.initialize(bootstrap);
    }

    @Override
    public void run(TimeLockServerConfiguration configuration, Environment environment) throws Exception {
        environment.getObjectMapper().registerModule(new Jdk8Module());
        environment.jersey().register(HttpRemotingJerseyFeature.DEFAULT);

        Observable<TimeLockRuntimeConfiguration> runtime = watchForever(
                configurationPath,
                Schedulers.io(),
                (path) -> liveReloader.getNewConfiguration(),
                t -> log.error("Could not deserialize runtime config from {}, blocking runtime config update",
                        configurationPath, t))
                .map(TimeLockConfigMigrator::createRuntimeConfiguration);
        runtime.subscribe(ignored -> log.info("Live reloaded config from {}", configurationPath));

        CombinedTimeLockServerConfiguration combined = TimeLockConfigMigrator.convert(configuration, environment);
        Consumer<Object> registrar = component -> environment.jersey().register(component);
        TimeLockAgent agent = combined.install().algorithm().createTimeLockAgent(
                combined.install(),
                runtime,
                combined.deprecated(),
                registrar);
        agent.createAndRegisterResources();
    }

    private static <T> Observable<T> watchForever(
            Path path, Scheduler scheduler, Function<Path, T> func, Consumer<Throwable> onError) throws Exception {
        return Observable.just(func.apply(path))
                .concatWith(Observable.interval(1, TimeUnit.SECONDS, scheduler)
                        .flatMapIterable(e -> {
                            try {
                                return Collections.singleton(func.apply(path));
                            } catch (Throwable t) {
                                onError.accept(t);
                                return Collections.emptySet();
                            }
                        })
                        .share())
                .distinctUntilChanged();
    }
}
