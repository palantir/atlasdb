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
package com.palantir.atlasdb.timelock.config;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

import io.dropwizard.Configuration;

public class TimeLockServerConfiguration extends Configuration {
    private final AtomixConfiguration atomix;
    private final ClusterConfiguration cluster;
    private final Set<String> clients;

    public TimeLockServerConfiguration(
            @JsonProperty(value = "atomix", required = false) AtomixConfiguration atomix,
            @JsonProperty(value = "cluster", required = true) ClusterConfiguration cluster,
            @JsonProperty(value = "clients", required = true) Set<String> clients) {
        Preconditions.checkState(!clients.isEmpty(), "'clients' should have at least one entry");
        checkAllClientNames(clients);

        this.atomix = MoreObjects.firstNonNull(atomix, AtomixConfiguration.DEFAULT);
        this.cluster = cluster;
        this.clients = clients;
    }

    public AtomixConfiguration atomix() {
        return atomix;
    }

    public ClusterConfiguration cluster() {
        return cluster;
    }

    public Set<String> clients() {
        return clients;
    }

    private static void checkAllClientNames(Set<String> clients) {
        clients.forEach(
                client -> Preconditions.checkState(client.matches("[a-zA-Z0-9_-]+"),
                        String.format("Client names must consist of 1 or more alphanumeric characters, dashes or "
                                + "underscores only; found '%s' which does not follow this", client))
        );
    }
}
