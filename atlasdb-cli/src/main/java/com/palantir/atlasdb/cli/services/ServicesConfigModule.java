/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.cli.services;

import java.io.File;
import java.io.IOException;

import javax.inject.Singleton;

import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbConfigs;

import dagger.Module;
import dagger.Provides;

@Module
public class ServicesConfigModule {

    private final ServicesConfig config;

    public static ServicesConfigModule create(File configFile, String configRoot, boolean rootIsPath) throws IOException {
        return ServicesConfigModule.create(AtlasDbConfigs.load(configFile, configRoot, rootIsPath));
    }

    public static ServicesConfigModule create(AtlasDbConfig atlasDbConfig) {
        return new ServicesConfigModule(ImmutableServicesConfig.builder().atlasDbConfig(atlasDbConfig).build());
    }

    public ServicesConfigModule(ServicesConfig config) {
        this.config = config;
    }

    @Provides
    @Singleton
    public ServicesConfig provideServicesConfig() { return config; }

    @Provides
    @Singleton
    public AtlasDbConfig provideAtlasDbConfig() { return config.atlasDbConfig(); }

}