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

import com.palantir.atlasdb.cas.CheckAndSetClient;
import com.palantir.atlasdb.cas.SimpleCheckAndSetResource;
import com.palantir.atlasdb.dropwizard.AtlasDbBundle;
import com.palantir.atlasdb.todo.SimpleTodoResource;
import com.palantir.atlasdb.todo.TodoClient;

import io.dropwizard.Application;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class AtlasDbEteServer extends Application<AtlasDbEteConfiguration> {
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
        TodoClient todoClient = new TodoClient(config.getAtlasConfig(), environment.jersey());
        environment.jersey().register(new SimpleTodoResource(todoClient));

        CheckAndSetClient checkAndSetClient = new CheckAndSetClient(config.getAtlasConfig(), environment.jersey());
        environment.jersey().register(new SimpleCheckAndSetResource(checkAndSetClient));
    }

    private void enableEnvironmentVariablesInConfig(Bootstrap<AtlasDbEteConfiguration> bootstrap) {
        bootstrap.setConfigurationSourceProvider(
                new SubstitutingSourceProvider(
                        bootstrap.getConfigurationSourceProvider(),
                        new EnvironmentVariableSubstitutor()));
    }
}
