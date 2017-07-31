/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.timelock.partition;

import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;

@ThreadSafe // Is immutable.
public class Assignment {
    @JsonProperty("clientsToHosts")
    private final ImmutableMultimap<String, String> clientsToHosts;
    @JsonProperty("hostsToClients")
    private final ImmutableMultimap<String, String> hostsToClients;

    private Assignment(
            @JsonProperty("clientsToHosts") Multimap<String, String> clientsToHosts,
            @JsonProperty("hostsToClients") Multimap<String, String> hostsToClients) {
        this.clientsToHosts = ImmutableMultimap.copyOf(clientsToHosts);
        this.hostsToClients = ImmutableMultimap.copyOf(hostsToClients);
    }

    @JsonIgnore
    public Set<String> getKnownClients() {
        return clientsToHosts.keySet();
    }

    @JsonIgnore
    public Set<String> getKnownHosts() {
        return hostsToClients.keySet();
    }

    @JsonIgnore
    public Set<String> getClientsForHost(String host) {
        return ImmutableSet.copyOf(hostsToClients.get(host));
    }

    @JsonIgnore
    public Set<String> getHostsForClient(String client) {
        return ImmutableSet.copyOf(clientsToHosts.get(client));
    }

    public static Assignment.Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "Assignment{" +
                "clientsToHosts=" + clientsToHosts +
                ", hostsToClients=" + hostsToClients +
                '}';
    }

    @NotThreadSafe
    public static class Builder {
        private final Multimap<String, String> clientsToHosts;
        private final Multimap<String, String> hostsToClients;

        private Builder() {
            clientsToHosts = MultimapBuilder.hashKeys().hashSetValues().build();
            hostsToClients = MultimapBuilder.hashKeys().hashSetValues().build();
        }

        public Builder addMapping(String client, String host) {
            clientsToHosts.put(client, host);
            hostsToClients.put(host, client);
            return this;
        }

        public Assignment build() {
            return new Assignment(clientsToHosts, hostsToClients);
        }
    }
}
