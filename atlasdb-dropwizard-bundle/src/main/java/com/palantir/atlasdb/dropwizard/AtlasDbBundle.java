/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.dropwizard;

import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;

public class AtlasDbBundle<T extends Configuration & AtlasDbConfigurationProvider> implements ConfiguredBundle<T> {
    @Override
    public void initialize(Bootstrap<?> bootstrap) {
        Class<T> configurationClass = (Class<T>) bootstrap.getApplication().getConfigurationClass();
        bootstrap.addCommand(new AtlasDbConfiguredCommand<T>(configurationClass));
    }

    @Override
    public void run(T configuration, Environment environment) throws Exception {
    }
}
