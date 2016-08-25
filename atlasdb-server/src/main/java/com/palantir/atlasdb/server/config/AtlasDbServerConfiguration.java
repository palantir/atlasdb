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
package com.palantir.atlasdb.server.config;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import io.dropwizard.Configuration;

public class AtlasDbServerConfiguration extends Configuration {
    private final ClusterConfig cluster;
    private final Set<ClientConfig> clients;

    public AtlasDbServerConfiguration(
            @JsonProperty(value = "cluster", required = true) ClusterConfig cluster,
            @JsonProperty(value = "clients", required = true) Set<ClientConfig> clients) {
        Preconditions.checkState(!clients.isEmpty(), "'clients' should have at least one entry");

        this.cluster = cluster;
        this.clients = clients;
    }

    public ClusterConfig cluster() {
        return cluster;
    }

    public Set<ClientConfig> clients() {
        return clients;
    }
}
