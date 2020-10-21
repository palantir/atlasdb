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
package com.palantir.atlasdb.timelock.paxos;

import java.util.Set;
import java.util.stream.Collectors;

public final class PaxosTimeLockUriUtils {
    private PaxosTimeLockUriUtils() {}

    public static Set<String> getLeaderPaxosUris(Set<String> addresses) {
        return getNamespacedUris(
                addresses, PaxosTimeLockConstants.INTERNAL_NAMESPACE, PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE);
    }

    public static Set<String> getClientPaxosUris(Set<String> addresses, String client) {
        return getNamespacedUris(
                addresses,
                PaxosTimeLockConstants.INTERNAL_NAMESPACE,
                PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE,
                client);
    }

    private static Set<String> getNamespacedUris(Set<String> addresses, String... suffixes) {
        String joinedSuffix = String.join("/", suffixes);
        return addresses.stream()
                .map(address -> String.join("/", address, joinedSuffix))
                .collect(Collectors.toSet());
    }
}
