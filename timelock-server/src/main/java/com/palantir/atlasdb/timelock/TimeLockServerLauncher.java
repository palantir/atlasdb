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
package com.palantir.atlasdb.timelock;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.palantir.atlasdb.timelock.config.CombinedTimeLockServerConfiguration;
import com.palantir.atlasdb.timelock.logging.NonBlockingFileAppenderFactory;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.conjure.java.api.config.service.UserAgent;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.conjure.java.server.jersey.ConjureJerseyFeature;
import com.palantir.refreshable.Refreshable;
import com.palantir.sls.versions.OrderableSlsVersion;
import com.palantir.timelock.config.TimeLockRuntimeConfiguration;
import com.palantir.timelock.paxos.TimeLockAgent;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;
import com.palantir.tritium.metrics.registry.TaggedMetricRegistry;
import io.dropwizard.Application;
import io.dropwizard.jackson.DiscoverableSubtypeResolver;
import io.dropwizard.jersey.optional.EmptyOptionalException;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;
import org.eclipse.jetty.util.component.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a way of launching an embedded TimeLock server using Dropwizard. Should only be used in tests.
 */
public class TimeLockServerLauncher extends Application<CombinedTimeLockServerConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(TimeLockServerLauncher.class);

    private static final UserAgent USER_AGENT = UserAgent.of(UserAgent.Agent.of("TimeLockServerLauncher", "0.0.0"));

    public static void main(String[] args) throws Exception {
        new TimeLockServerLauncher().run(args);
    }

    private final TaggedMetricRegistry taggedMetricRegistry = new DefaultTaggedMetricRegistry();
    private final SettableFuture<Void> shutdownFuture = SettableFuture.create();

    @Override
    public void initialize(Bootstrap<CombinedTimeLockServerConfiguration> bootstrap) {
        MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(
                "AtlasDbTest" + UUID.randomUUID().toString());
        bootstrap.setMetricRegistry(metricRegistry);
        bootstrap.setObjectMapper(ObjectMappers.newServerObjectMapper());

        bootstrap.getObjectMapper().registerSubtypes(NonBlockingFileAppenderFactory.class);
        bootstrap.getObjectMapper().setSubtypeResolver(new DiscoverableSubtypeResolver());
        bootstrap.getObjectMapper().registerModule(new Jdk8Module());
        super.initialize(bootstrap);
    }

    @Override
    public void run(CombinedTimeLockServerConfiguration configuration, Environment environment)
            throws JsonProcessingException {
        environment.getObjectMapper().registerModule(new Jdk8Module()).registerModule(new JavaTimeModule());
        environment.jersey().register(ConjureJerseyFeature.INSTANCE);
        environment.jersey().register(new EmptyOptionalTo204ExceptionMapper());

        MetricsManager metricsManager = MetricsManagers.of(environment.metrics(), taggedMetricRegistry);
        Consumer<Object> registrar = component -> environment.jersey().register(component);

        log.info(
                "Paxos configuration\n{}",
                environment
                        .getObjectMapper()
                        .writerWithDefaultPrettyPrinter()
                        .writeValueAsString(configuration.install().paxos()));
        TimeLockRuntimeConfiguration runtime = configuration.runtime();
        TimeLockAgent timeLockAgent = TimeLockAgent.create(
                metricsManager,
                configuration.install(),
                Refreshable.only(runtime), // this won't actually live reload
                runtime.cluster(),
                USER_AGENT,
                CombinedTimeLockServerConfiguration.threadPoolSize(),
                CombinedTimeLockServerConfiguration.blockingTimeoutMs(),
                registrar,
                Optional.empty(),
                OrderableSlsVersion.valueOf("0.0.0"),
                environment.getObjectMapper());

        environment.lifecycle().manage(new Managed() {
            @Override
            public void start() {}

            @Override
            public void stop() {}
        });
        environment.lifecycle().addLifeCycleListener(new LifeCycle.Listener() {
            @Override
            public void lifeCycleStarting(LifeCycle event) {}

            @Override
            public void lifeCycleStarted(LifeCycle event) {}

            @Override
            public void lifeCycleFailure(LifeCycle event, Throwable cause) {
                shutdownFuture.setException(cause);
            }

            @Override
            public void lifeCycleStopping(LifeCycle event) {}

            @Override
            public void lifeCycleStopped(LifeCycle event) {
                timeLockAgent.shutdown();
                shutdownFuture.set(null);
            }
        });
    }

    public TaggedMetricRegistry taggedMetricRegistry() {
        return taggedMetricRegistry;
    }

    public ListenableFuture<Void> shutdownFuture() {
        return shutdownFuture;
    }

    @Provider
    private static final class EmptyOptionalTo204ExceptionMapper implements ExceptionMapper<EmptyOptionalException> {
        @Override
        public Response toResponse(EmptyOptionalException exception) {
            return Response.noContent().build();
        }
    }
}
