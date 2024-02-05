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

import com.google.common.collect.ImmutableList;
import com.google.common.net.HostAndPort;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public final class RedirectRetryTargeter {
    private static final SafeLogger log = SafeLoggerFactory.get(RedirectRetryTargeter.class);
    private final List<URL> otherServers;
    private final Map<HostAndPort, URL> hostAndPortToUrls;

    private RedirectRetryTargeter(List<URL> otherServers) {
        this.otherServers = otherServers;
        // Not using KeyedStream or similar, because we need to provide a merge function.
        // It's acceptable to pick any URL as long as these are *correct* for the purpose of retry redirection.
        this.hostAndPortToUrls = otherServers.stream()
                .collect(Collectors.toMap(
                        url -> HostAndPort.fromParts(url.getHost(), url.getPort()), url -> url, (url1, url2) -> {
                            log.info(
                                    "Duplicate host and port. Keeping the first URL.",
                                    // Safe because they come from config
                                    SafeArg.of("url1", url1),
                                    SafeArg.of("url2", url2));
                            return url1;
                        }));
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
            return Optional.ofNullable(hostAndPortToUrls.get(leader));
        }
        return Optional.of(otherServers.get(ThreadLocalRandom.current().nextInt(otherServers.size())));
    }
}
