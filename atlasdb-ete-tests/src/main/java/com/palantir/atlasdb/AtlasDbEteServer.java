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
package com.palantir.atlasdb;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.SharedMetricRegistries;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cas.CheckAndSetClient;
import com.palantir.atlasdb.cas.CheckAndSetSchema;
import com.palantir.atlasdb.cas.SimpleCheckAndSetResource;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.dropwizard.AtlasDbBundle;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.http.NotInitializedExceptionMapper;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.todo.SimpleTodoResource;
import com.palantir.atlasdb.todo.TodoClient;
import com.palantir.atlasdb.todo.TodoSchema;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.remoting3.servers.jersey.HttpRemotingJerseyFeature;
import com.palantir.tritium.metrics.registry.DefaultTaggedMetricRegistry;

import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class AtlasDbEteServer extends Application<AtlasDbEteConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbEteServer.class);
    private static final long CREATE_TRANSACTION_MANAGER_MAX_WAIT_TIME_SECS = 60;
    private static final long CREATE_TRANSACTION_MANAGER_POLL_INTERVAL_SECS = 5;
    private static final Set<Schema> ETE_SCHEMAS = ImmutableSet.of(
            CheckAndSetSchema.getSchema(),
            TodoSchema.getSchema());

    public static void main(String[] args) throws Exception {
        new AtlasDbEteServer().run(args);
    }

    @Override
    public void initialize(Bootstrap<AtlasDbEteConfiguration> bootstrap) {
        bootstrap.setMetricRegistry(SharedMetricRegistries.getOrCreate("AtlasDbTest"));
        enableEnvironmentVariablesInConfig(bootstrap);
        bootstrap.addBundle(new AtlasDbBundle<>());
        bootstrap.getObjectMapper().registerModule(new Jdk8Module());
    }

    @Override
    public void run(AtlasDbEteConfiguration config, final Environment environment) throws Exception {
        TransactionManager transactionManager = tryToCreateTransactionManager(config, environment);
        environment.jersey().register(new SimpleTodoResource(new TodoClient(transactionManager)));
        environment.jersey().register(new SimpleCheckAndSetResource(new CheckAndSetClient(transactionManager)));
        environment.jersey().register(HttpRemotingJerseyFeature.INSTANCE);
        environment.jersey().register(new NotInitializedExceptionMapper());
    }

    private TransactionManager tryToCreateTransactionManager(AtlasDbEteConfiguration config, Environment environment)
            throws InterruptedException {
        if (config.getAtlasDbConfig().initializeAsync()) {
            return createTransactionManager(config.getAtlasDbConfig(), config.getAtlasDbRuntimeConfig(), environment);
        } else {
            return createTransactionManagerWithRetry(config.getAtlasDbConfig(),
                    config.getAtlasDbRuntimeConfig(),
                    environment);
        }
    }

    private TransactionManager createTransactionManagerWithRetry(AtlasDbConfig config,
            Optional<AtlasDbRuntimeConfig> atlasDbRuntimeConfig,
            Environment environment)
            throws InterruptedException {
        Stopwatch sw = Stopwatch.createStarted();
        while (sw.elapsed(TimeUnit.SECONDS) < CREATE_TRANSACTION_MANAGER_MAX_WAIT_TIME_SECS) {
            try {
                return createTransactionManager(config, atlasDbRuntimeConfig, environment);
            } catch (RuntimeException e) {
                log.warn("An error occurred while trying to create transaction manager. Retrying...", e);
                Thread.sleep(CREATE_TRANSACTION_MANAGER_POLL_INTERVAL_SECS);
            }
        }
        throw new IllegalStateException("Timed-out because we were unable to create transaction manager");
    }

    private TransactionManager createTransactionManager(AtlasDbConfig config,
            Optional<AtlasDbRuntimeConfig> atlasDbRuntimeConfigOptional, Environment environment) {
        return TransactionManagers.builder()
                .config(config)
                .userAgent("ete test")
                .globalMetricsRegistry(environment.metrics())
                .globalTaggedMetricRegistry(DefaultTaggedMetricRegistry.getDefault())
                .registrar(environment.jersey()::register)
                .addAllSchemas(ETE_SCHEMAS)
                .build()
                .serializable();
    }

    private void enableEnvironmentVariablesInConfig(Bootstrap<AtlasDbEteConfiguration> bootstrap) {
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(
                        bootstrap.getConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor()));
    }
}
