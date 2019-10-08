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

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

    public static Set<String> getRemoteServerPaths(TimeLockInstallConfiguration install) {
        return addProtocols(getRemoteServerAddresses(install));
    }

    public static ImmutableSet<String> getClusterAddresses(TimeLockInstallConfiguration install) {
        return ImmutableSet.copyOf(getClusterConfiguration(install).clusterMembers());
    }

    public static Set<String> getRemoteServerAddresses(TimeLockInstallConfiguration install) {
        return Sets.difference(getClusterAddresses(install),
                ImmutableSet.of(install.cluster().localServer()));
    }

    public static ClusterConfiguration getClusterConfiguration(TimeLockInstallConfiguration install) {
        return install.cluster();
    }

    public static SslConfiguration getSslConfiguration(TimeLockInstallConfiguration install) {
        return getClusterConfiguration(install).cluster().security().get();
    }

    public static String addProtocol(String address) {
        return "https://" + address;
    }

    public static Set<String> addProtocols(Set<String> addresses) {
        return addresses.stream()
                .map(PaxosRemotingUtils::addProtocol)
                .collect(Collectors.toSet());
    }
}
