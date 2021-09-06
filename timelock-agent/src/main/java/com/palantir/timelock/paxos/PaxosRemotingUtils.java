/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.timelock.paxos;

import com.google.common.collect.ImmutableList;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.timelock.config.ClusterConfiguration;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.ws.rs.core.UriBuilder;

public final class PaxosRemotingUtils {
    private PaxosRemotingUtils() {
        // utility class
    }

    public static <T> int getQuorumSize(Collection<T> elements) {
        return elements.size() / 2 + 1;
    }

    public static List<String> getRemoteServerPaths(ClusterConfiguration cluster) {
        return addProtocols(cluster, getRemoteServerAddresses(cluster));
    }

    public static ImmutableList<String> getClusterAddresses(ClusterConfiguration cluster) {
        return ImmutableList.copyOf(cluster.clusterMembers());
    }

    public static List<String> getRemoteServerAddresses(ClusterConfiguration cluster) {
        List<String> result = new ArrayList<>(getClusterAddresses(cluster));
        result.remove(cluster.localServer());
        return ImmutableList.copyOf(result);
    }

    public static Optional<SslConfiguration> getSslConfigurationOptional(ClusterConfiguration cluster) {
        return cluster.cluster().security();
    }

    public static String addProtocol(ClusterConfiguration cluster, String address) {
        String protocolPrefix = getSslConfigurationOptional(cluster).isPresent() ? "https://" : "http://";
        return protocolPrefix + address;
    }

    public static List<String> addProtocols(ClusterConfiguration cluster, List<String> addresses) {
        return addresses.stream().map(address -> addProtocol(cluster, address)).collect(Collectors.toList());
    }

    public static URL convertAddressToUrl(ClusterConfiguration cluster) {
        try {
            return UriBuilder.fromPath(addProtocol(cluster, cluster.localServer()))
                    .build()
                    .toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<URL> convertAddressesToUrls(ClusterConfiguration cluster) {
        return cluster.clusterMembers().stream()
                .map(address -> convertAddressToUrl(cluster))
                .collect(Collectors.toList());
    }
}
