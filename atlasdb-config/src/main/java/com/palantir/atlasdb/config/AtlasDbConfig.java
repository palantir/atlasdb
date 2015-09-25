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
package com.palantir.atlasdb.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

public final class AtlasDbConfig {

    private final KeyValueServiceConfig keyValueService;
    private final Optional<LeaderConfig> leader;
    private final ServerListConfig lock;
    private final ServerListConfig timestamp;

    public AtlasDbConfig(
            @JsonProperty("keyValueService") KeyValueServiceConfig keyValueService,
            @JsonProperty("leader") Optional<LeaderConfig> leader,
            @JsonProperty("lock") ServerListConfig lock,
            @JsonProperty("timestamp") ServerListConfig timestamp) {
        this.keyValueService = keyValueService;
        this.leader = leader;
        this.lock = lock;
        this.timestamp = timestamp;
    }

    public KeyValueServiceConfig getKeyValueService() {
        return keyValueService;
    }

    public Optional<LeaderConfig> getLeader() {
        return leader;
    }

    public ServerListConfig getLock() {
        return lock;
    }

    public ServerListConfig getTimestamp() {
        return timestamp;
    }

}
