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

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

public final class ServerListConfig {

    private final Set<String> servers;

    public ServerListConfig(@JsonProperty("servers") Set<String> servers) {
        Preconditions.checkNotNull(servers, "Server list must not be empty");
        Preconditions.checkArgument(!servers.isEmpty(), "Server list must not be empty");
        this.servers = ImmutableSet.copyOf(servers);
    }

    public Set<String> getServers() {
        return servers;
    }

}
