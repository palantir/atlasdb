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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import java.net.InetSocketAddress;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Denylist {
    private static final Logger log = LoggerFactory.getLogger(Denylist.class);

    private final CassandraKeyValueServiceConfig config;
    private final Clock clock;

    private Map<InetSocketAddress, Long> denylist;

    public Denylist(CassandraKeyValueServiceConfig config) {
        this(config, Clock.systemUTC());
    }

    @VisibleForTesting
    Denylist(CassandraKeyValueServiceConfig config, Clock clock) {
        this.config = config;
        this.denylist = new ConcurrentHashMap<>();
        this.clock = clock;
    }

    void checkAndUpdate(Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {
        // Check denylist and re-integrate or continue to wait as necessary
        for (Map.Entry<InetSocketAddress, Long> denylistedEntry : denylist.entrySet()) {
            if (coolOffPeriodExpired(denylistedEntry)) {
                InetSocketAddress host = denylistedEntry.getKey();
                if (!pools.containsKey(host)) {
                    // Probably the pool changed underneath us
                    denylist.remove(host);
                    log.info(
                            "Removing host {} from the denylist as it wasn't found in the pool.",
                            SafeArg.of("host", CassandraLogHelper.host(host)));
                } else if (isHostHealthy(pools.get(host))) {
                    denylist.remove(host);
                    log.info(
                            "Added host {} back into the pool after a waiting period and successful health check.",
                            SafeArg.of("host", CassandraLogHelper.host(host)));
                }
            }
        }
    }

    private boolean coolOffPeriodExpired(Map.Entry<InetSocketAddress, Long> denylistedEntry) {
        long backoffTimeMillis = TimeUnit.SECONDS.toMillis(config.unresponsiveHostBackoffTimeSeconds());
        return denylistedEntry.getValue() + backoffTimeMillis < clock.millis();
    }

    private boolean isHostHealthy(CassandraClientPoolingContainer container) {
        try {
            container.runWithPooledResource(CassandraUtils.getDescribeRing(config));
            container.runWithPooledResource(CassandraUtils.getValidatePartitioner(config));
            return true;
        } catch (Exception e) {
            log.info(
                    "We tried to add denylisted host '{}' back into the pool, but got an exception"
                            + " that caused us to distrust this host further. Exception message was: {} : {}",
                    SafeArg.of("host", CassandraLogHelper.host(container.getHost())),
                    SafeArg.of("exceptionClass", e.getClass().getCanonicalName()),
                    UnsafeArg.of("exceptionMessage", e.getMessage()),
                    e);
            return false;
        }
    }

    public Set<InetSocketAddress> filterBlacklistedHostsFrom(Collection<InetSocketAddress> potentialHosts) {
        return Sets.difference(ImmutableSet.copyOf(potentialHosts), denylist.keySet());
    }

    boolean contains(InetSocketAddress host) {
        return denylist.containsKey(host);
    }

    public void add(InetSocketAddress host) {
        denylist.put(host, clock.millis());
        log.info("Blacklisted host '{}'", SafeArg.of("badHost", CassandraLogHelper.host(host)));
    }

    void addAll(Set<InetSocketAddress> hosts) {
        hosts.forEach(this::add);
    }

    public void remove(InetSocketAddress host) {
        denylist.remove(host);
    }

    void removeAll() {
        denylist.clear();
    }

    int size() {
        return denylist.size();
    }

    public String describeBlacklistedHosts() {
        return denylist.keySet().toString();
    }

    public List<String> denylistDetails() {
        return denylist.entrySet().stream()
                .map(denylistedHostToBlacklistTime -> String.format(
                        "host: %s was denylisted at %s",
                        CassandraLogHelper.host(denylistedHostToBlacklistTime.getKey()),
                        denylistedHostToBlacklistTime.getValue().longValue()))
                .collect(Collectors.toList());
    }
}
