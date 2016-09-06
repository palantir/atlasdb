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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.palantir.atlasdb.config.AtlasDbConfig;
import com.palantir.atlasdb.dropwizard.AtlasDbConfigurationProvider;
import com.palantir.remoting.ssl.SslConfiguration;

import io.dropwizard.Configuration;

public class AtlasDbEteConfiguration extends Configuration implements AtlasDbConfigurationProvider {
    private final AtlasDbConfig atlasdb;

    public AtlasDbEteConfiguration(@JsonProperty("atlasdb") AtlasDbConfig atlasdb) {
        this.atlasdb = atlasdb;
    }

    @Override
    public AtlasDbConfig getAtlasDbConfig() {
        return atlasdb;
    }

    @Override
    public Optional<SslConfiguration> getLeaderSslConfiguration() {
        return Optional.absent();
    }
}
