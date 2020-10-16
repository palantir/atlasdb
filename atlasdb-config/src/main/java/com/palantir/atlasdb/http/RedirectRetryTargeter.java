/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.http;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public final class RedirectRetryTargeter {
    private final List<URL> otherServers;
    private final BiMap<HostAndPort, URL> urlsToHostAndPort;

    private RedirectRetryTargeter(List<URL> otherServers) {
        this.otherServers = otherServers;
        this.urlsToHostAndPort = KeyedStream.of(otherServers)
                .mapKeys(url -> HostAndPort.fromParts(url.getHost(), url.getPort()))
                .collectTo(HashBiMap::create);
    }

    public static RedirectRetryTargeter create(URL localServer, List<URL> clusterUrls) {
        Preconditions.checkArgument(
                clusterUrls.contains(localServer),
                "Local server not found in the list of cluster URLs.",
                SafeArg.of("localServer", localServer),
                SafeArg.of("clusterUrls", clusterUrls));

        if (clusterUrls.size() == 1) {
            return new RedirectRetryTargeter(ImmutableList.of());
        }
        List<URL> otherServers = clusterUrls.stream()
                .filter(url -> !Objects.equals(localServer, url))
                .collect(Collectors.toList());

        return new RedirectRetryTargeter(otherServers);
    }

    public Optional<URL> redirectRequest(Optional<HostAndPort> leaderHint) {
        if (otherServers.isEmpty()) {
            return Optional.empty();
        }

        if (leaderHint.isPresent()) {
            HostAndPort leader = leaderHint.get();
            return Optional.ofNullable(urlsToHostAndPort.get(leader));
        }
        return Optional.of(otherServers.get(ThreadLocalRandom.current().nextInt(otherServers.size())));
    }
}
