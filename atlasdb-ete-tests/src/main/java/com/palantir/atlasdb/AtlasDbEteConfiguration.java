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
package com.palantir.atlasdb;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.config.AtlasDbRuntimeConfig;
import com.palantir.atlasdb.dropwizard.AtlasDbConfigurationProvider;

import io.dropwizard.Configuration;

public class AtlasDbEteConfiguration extends Configuration implements AtlasDbConfigurationProvider {
    private final AtlasDbConfig atlasdb;
    private final Optional<AtlasDbRuntimeConfig> atlasdbRuntime;

    public AtlasDbEteConfiguration(@JsonProperty("atlasdb") AtlasDbConfig atlasdb,
            @JsonProperty("atlasDbRuntime") Optional<AtlasDbRuntimeConfig> atlasDbRuntimeConfig) {
        this.atlasdb = atlasdb;
        this.atlasdbRuntime = atlasDbRuntimeConfig;
    }

    @Override
    public AtlasDbConfig getAtlasDbConfig() {
        return atlasdb;
    }

    @Override
    public Optional<AtlasDbRuntimeConfig> getAtlasDbRuntimeConfig() {
        return atlasdbRuntime;
    }

}
