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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.UriBuilder;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.timelock.config.ClusterConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;

public final class PaxosRemotingUtils {
    private PaxosRemotingUtils() {
        // utility class
    }

    public static <T> int getQuorumSize(Collection<T> elements) {
        return elements.size() / 2 + 1;
    }

    public static Set<String> getRemoteServerPaths(
            TimeLockInstallConfiguration install) {
        return getRemoteServerPaths(install, false);
    }

    public static Set<String> getRemoteServerPaths(
            TimeLockInstallConfiguration install, boolean useDedicatedIfAvailable) {
        return addProtocols(install, getRemoteServerAddresses(install, useDedicatedIfAvailable), useDedicatedIfAvailable);
    }

    public static ImmutableSet<String> getClusterAddresses(TimeLockInstallConfiguration install) {
        return getClusterAddresses(install, false);
    }

    public static ImmutableSet<String> getClusterAddresses(
            TimeLockInstallConfiguration install,
            boolean useDedicatedIfAvailable) {
        return ImmutableSet.copyOf(getClusterConfiguration(install, useDedicatedIfAvailable).clusterMembers());
    }

    public static Set<String> getRemoteServerAddresses(TimeLockInstallConfiguration install) {
        return getRemoteServerAddresses(install, false);
    }

    public static Set<String> getRemoteServerAddresses(
            TimeLockInstallConfiguration install,
            boolean useDedicatedIfAvailable) {
        return Sets.difference(getClusterAddresses(install, useDedicatedIfAvailable),
                ImmutableSet.of(getClusterConfiguration(install, useDedicatedIfAvailable).localServer()));
    }

    public static ClusterConfiguration getClusterConfiguration(TimeLockInstallConfiguration install) {
        return getClusterConfiguration(install, false);
    }

    public static ClusterConfiguration getClusterConfiguration(
            TimeLockInstallConfiguration install,
            boolean useDedicatedIfAvailable) {
        return install.dedicatedPaxosConfig()
                .filter(dedicatedConfig -> useDedicatedIfAvailable && dedicatedConfig.enabled())
                .map(TimeLockInstallConfiguration.DedicatedPaxosConfiguration::cluster)
                .orElse(install.cluster());
    }

    public static Optional<SslConfiguration> getSslConfigurationOptional(TimeLockInstallConfiguration install) {
        return getSslConfigurationOptional(install, false);
    }

    public static Optional<SslConfiguration> getSslConfigurationOptional(
            TimeLockInstallConfiguration install, boolean useDedicatedIfAvailable) {
        return getClusterConfiguration(install, useDedicatedIfAvailable).cluster().security();
    }

    public static String addProtocol(TimeLockInstallConfiguration install, String address) {
        return addProtocol(install, address, false);
    }

    public static String addProtocol(TimeLockInstallConfiguration install,
            String address, boolean useDedicatedIfAvailable) {
        String protocolPrefix = getSslConfigurationOptional(install, useDedicatedIfAvailable)
                .isPresent() ? "https://" : "http://";
        return protocolPrefix + address;
    }

    public static Set<String> addProtocols(TimeLockInstallConfiguration install, Set<String> addresses) {
        return addProtocols(install, addresses, false);
    }

    public static Set<String> addProtocols(
            TimeLockInstallConfiguration install, Set<String> addresses, boolean useDedicatedIfAvailable) {
        return addresses.stream()
                .map(address -> addProtocol(install, address, useDedicatedIfAvailable))
                .collect(Collectors.toSet());
    }

    public static URL convertAddressToUrl(TimeLockInstallConfiguration install, String address) {
        try {
            return UriBuilder.fromPath(addProtocol(install, address)).build().toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<URL> convertAddressesToUrls(TimeLockInstallConfiguration install, List<String> addresses) {
        return addresses.stream()
                .map(address -> convertAddressToUrl(install, address))
                .collect(Collectors.toList());
    }
}
