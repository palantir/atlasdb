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

<<<<<<< 7033b8fc57203bf309772ac48101c6126fb91d56
=======
import javax.net.ssl.SSLSocketFactory;

import com.google.common.base.Optional;
>>>>>>> merge develop into perf cli branch (#820)
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
    private static final boolean DONT_SHOW_HIDDEN_TABLES = false;
<<<<<<< 7033b8fc57203bf309772ac48101c6126fb91d56
=======
    private static final Optional<SSLSocketFactory> NO_SSL = Optional.absent();
>>>>>>> merge develop into perf cli branch (#820)
    private static final Set<Schema> ETE_SCHEMAS = ImmutableSet.of(
            CheckAndSetSchema.getSchema(),
            TodoSchema.getSchema());

    public static void main(String[] args) throws Exception {
        new AtlasDbEteServer().run(args);
    }

    @Override
    public void initialize(Bootstrap<AtlasDbEteConfiguration> bootstrap) {
        enableEnvironmentVariablesInConfig(bootstrap);
        bootstrap.addBundle(new AtlasDbBundle<>());
    }

    @Override
    public void run(AtlasDbEteConfiguration config, final Environment environment) throws Exception {
<<<<<<< 7033b8fc57203bf309772ac48101c6126fb91d56
        TransactionManager transactionManager = TransactionManagers.create(config.getAtlasDbConfig(), ETE_SCHEMAS, environment.jersey()::register, DONT_SHOW_HIDDEN_TABLES);
=======
        TransactionManager transactionManager = TransactionManagers.create(config.getAtlasDbConfig(), NO_SSL, ETE_SCHEMAS, environment.jersey()::register, DONT_SHOW_HIDDEN_TABLES);
>>>>>>> merge develop into perf cli branch (#820)
        environment.jersey().register(new SimpleTodoResource(new TodoClient(transactionManager)));
        environment.jersey().register(new SimpleCheckAndSetResource(new CheckAndSetClient(transactionManager)));
    }

    private void enableEnvironmentVariablesInConfig(Bootstrap<AtlasDbEteConfiguration> bootstrap) {
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(
                        bootstrap.getConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor()));
    }
}
