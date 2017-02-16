/**
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
package com.palantir.atlasdb;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cas.CheckAndSetClient;
import com.palantir.atlasdb.cas.CheckAndSetSchema;
import com.palantir.atlasdb.cas.SimpleCheckAndSetResource;
import com.palantir.atlasdb.dropwizard.AtlasDbBundle;
import com.palantir.atlasdb.factory.TransactionManagers;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.todo.SimpleTodoResource;
import com.palantir.atlasdb.todo.TodoClient;
import com.palantir.atlasdb.todo.TodoSchema;
import com.palantir.atlasdb.transaction.api.TransactionManager;

import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class AtlasDbEteServer extends Application<AtlasDbEteConfiguration> {
    private static final Logger log = LoggerFactory.getLogger(AtlasDbEteServer.class);

    private static final long CREATE_TRANSACTION_MANAGER_MAX_WAIT_TIME_SECS = 60;
    private static final long CREATE_TRANSACTION_MANAGER_POLL_INTERVAL_SECS = 5;
    private static final boolean DONT_SHOW_HIDDEN_TABLES = false;
    private static final Set<Schema> ETE_SCHEMAS = ImmutableSet.of(
            CheckAndSetSchema.getSchema(),
            TodoSchema.getSchema());

    public static void main(String[] args) throws Exception {
        new AtlasDbEteServer().run(args);
    }

    public static void mainNonBlocking(String[] args) {
        Executors.newSingleThreadExecutor().execute(() -> {
            try {
                new AtlasDbEteServer().run(args);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

    @Override
    public void initialize(Bootstrap<AtlasDbEteConfiguration> bootstrap) {
        enableEnvironmentVariablesInConfig(bootstrap);
        bootstrap.addBundle(new AtlasDbBundle<>());
    }

    @Override
    public void run(AtlasDbEteConfiguration config, final Environment environment) throws Exception {
        TransactionManager transactionManager = createTransactionManager(config, environment);
        environment.jersey().register(new SimpleTodoResource(new TodoClient(transactionManager)));
        environment.jersey().register(new SimpleCheckAndSetResource(new CheckAndSetClient(transactionManager)));
    }

    private TransactionManager createTransactionManager(AtlasDbEteConfiguration config, Environment environment)
            throws InterruptedException {
        Stopwatch sw = Stopwatch.createStarted();
        while (sw.elapsed(TimeUnit.SECONDS) < CREATE_TRANSACTION_MANAGER_MAX_WAIT_TIME_SECS) {
            try {
                return TransactionManagers.create(
                        config.getAtlasDbConfig(),
                        ETE_SCHEMAS,
                        environment.jersey()::register,
                        DONT_SHOW_HIDDEN_TABLES);
            } catch (RuntimeException e) {
                log.warn("An error occurred while trying to create transaction manager. Retrying...", e);
                Thread.sleep(CREATE_TRANSACTION_MANAGER_POLL_INTERVAL_SECS);
            }
        }
        throw new IllegalStateException("Timed-out because we were unable to create transaction manager");
    }

    private void enableEnvironmentVariablesInConfig(Bootstrap<AtlasDbEteConfiguration> bootstrap) {
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(
                        bootstrap.getConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor()));
    }
}
