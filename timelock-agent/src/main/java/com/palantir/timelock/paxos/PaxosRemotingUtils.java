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
import com.google.common.collect.Lists;
import com.palantir.conjure.java.api.config.ssl.SslConfiguration;
import com.palantir.timelock.config.ClusterConfiguration;
import com.palantir.timelock.config.TimeLockInstallConfiguration;
import java.net.MalformedURLException;
import java.net.URL;
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

    public static List<String> getRemoteServerPaths(TimeLockInstallConfiguration install) {
        return addProtocols(install, getRemoteServerAddresses(install));
    }

    public static ImmutableList<String> getClusterAddresses(TimeLockInstallConfiguration install) {
        return ImmutableList.copyOf(getClusterConfiguration(install).clusterMembers());
    }

    public static List<String> getRemoteServerAddresses(TimeLockInstallConfiguration install) {
        List<String> result = Lists.newArrayList(getClusterAddresses(install));
        result.remove(install.cluster().localServer());
        return ImmutableList.copyOf(result);
    }

    public static ClusterConfiguration getClusterConfiguration(TimeLockInstallConfiguration install) {
        return install.cluster();
    }

    public static Optional<SslConfiguration> getSslConfigurationOptional(TimeLockInstallConfiguration install) {
        return getClusterConfiguration(install).cluster().security();
    }

    public static String addProtocol(TimeLockInstallConfiguration install, String address) {
        String protocolPrefix = getSslConfigurationOptional(install).isPresent() ? "https://" : "http://";
        return protocolPrefix + address;
    }

    public static List<String> addProtocols(TimeLockInstallConfiguration install, List<String> addresses) {
        return addresses.stream()
                .map(address -> addProtocol(install, address))
                .collect(Collectors.toList());
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
