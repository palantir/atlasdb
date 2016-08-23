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
package com.palantir.atlasdb.server;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

import io.dropwizard.Configuration;

public class AtlasDbServerConfiguration extends Configuration {
    private final LeaderConfig leaderConfig;
    private final Map<String, KeyValueServiceConfig> keyspaces;

    public AtlasDbServerConfiguration(
            @JsonProperty("leader") LeaderConfig leaderConfig,
            @JsonProperty("keyspaces") Map<String, KeyValueServiceConfig> keyspaces) {
        Preconditions.checkState(!keyspaces.isEmpty(), "'keyspaces' should have at least one entry");

        this.leaderConfig = leaderConfig;
        this.keyspaces = keyspaces;
    }

    @JsonProperty("leader")
    public LeaderConfig getLeaderConfig() {
        return leaderConfig;
    }

    @JsonProperty("keyspaces")
    public Map<String, KeyValueServiceConfig> getKeyspaces() {
        return keyspaces;
    }
}
