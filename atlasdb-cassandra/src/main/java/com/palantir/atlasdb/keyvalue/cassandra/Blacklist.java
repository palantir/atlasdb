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
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Clock;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Blacklist {
    private static final SafeLogger log = SafeLoggerFactory.get(Blacklist.class);

    private final CassandraKeyValueServiceConfig config;
    private final Clock clock;

    private Map<CassandraServer, Long> blacklist;

    public Blacklist(CassandraKeyValueServiceConfig config) {
        this(config, Clock.systemUTC());
    }

    @VisibleForTesting
    Blacklist(CassandraKeyValueServiceConfig config, Clock clock) {
        this.config = config;
        this.blacklist = new ConcurrentHashMap<>();
        this.clock = clock;
    }

    void checkAndUpdate(Map<CassandraServer, CassandraClientPoolingContainer> pools) {
        // Check blacklist and re-integrate or continue to wait as necessary
        Iterator<Entry<CassandraServer, Long>> blacklistIterator =
                blacklist.entrySet().iterator();
        while (blacklistIterator.hasNext()) {
            Map.Entry<CassandraServer, Long> blacklistedEntry = blacklistIterator.next();
            if (coolOffPeriodExpired(blacklistedEntry)) {
                CassandraServer cassandraServer = blacklistedEntry.getKey();
                if (!pools.containsKey(cassandraServer)) {
                    // Probably the pool changed underneath us
                    blacklistIterator.remove();
                    log.info(
                            "Removing cassandraServer {} from the blacklist as it wasn't found in the pool.",
                            SafeArg.of("cassandraServer", CassandraLogHelper.cassandraServer(cassandraServer)));
                } else if (isHostHealthy(pools.get(cassandraServer))) {
                    blacklistIterator.remove();
                    log.info(
                            "Added cassandraServer {} back into the pool after a waiting period and successful health"
                                    + " check.",
                            SafeArg.of("cassandraServer", CassandraLogHelper.cassandraServer(cassandraServer)));
                }
            }
        }
    }

    private boolean coolOffPeriodExpired(Map.Entry<CassandraServer, Long> blacklistedEntry) {
        long backoffTimeMillis = TimeUnit.SECONDS.toMillis(config.unresponsiveHostBackoffTimeSeconds());
        return blacklistedEntry.getValue() + backoffTimeMillis < clock.millis();
    }

    private boolean isHostHealthy(CassandraClientPoolingContainer container) {
        try {
            container.runWithPooledResource(CassandraUtils.getDescribeRing(config));
            container.runWithPooledResource(CassandraUtils.getValidatePartitioner(config));
            return true;
        } catch (Exception e) {
            log.info(
                    "We tried to add blacklisted host '{}' back into the pool, but got an exception"
                            + " that caused us to distrust this host further. Exception message was: {} : {}",
                    SafeArg.of("host", CassandraLogHelper.cassandraServer(container.getCassandraServer())),
                    SafeArg.of("exceptionClass", e.getClass().getCanonicalName()),
                    UnsafeArg.of("exceptionMessage", e.getMessage()),
                    e);
            return false;
        }
    }

    public Set<CassandraServer> filterBlacklistedHostsFrom(Collection<CassandraServer> potentialHosts) {
        return Sets.difference(ImmutableSet.copyOf(potentialHosts), blacklist.keySet());
    }

    boolean contains(CassandraServer cassandraServer) {
        return blacklist.containsKey(cassandraServer);
    }

    public void add(CassandraServer cassandraServer) {
        blacklist.put(cassandraServer, clock.millis());
        log.info(
                "Blacklisted cassandraServer '{}'",
                SafeArg.of("badHost", CassandraLogHelper.cassandraServer(cassandraServer)),
                SafeArg.of("proxy", CassandraLogHelper.host(cassandraServer.proxy())));
    }

    void addAll(Set<CassandraServer> hosts) {
        hosts.forEach(this::add);
    }

    public void remove(CassandraServer host) {
        blacklist.remove(host);
        log.info(
                "Remove blacklisted host '{}'",
                SafeArg.of("badHost", CassandraLogHelper.cassandraServer(host)),
                SafeArg.of("proxy", CassandraLogHelper.host(host.proxy())));
    }

    void removeAll() {
        blacklist.clear();
    }

    public int size() {
        return blacklist.size();
    }

    public String describeBlacklistedHosts() {
        return blacklist.keySet().toString();
    }

    public List<String> blacklistDetails() {
        return blacklist.entrySet().stream()
                .map(blacklistedHostToBlacklistTime -> String.format(
                        "host: %s was blacklisted at %s",
                        CassandraLogHelper.cassandraServer(blacklistedHostToBlacklistTime.getKey()),
                        blacklistedHostToBlacklistTime.getValue().longValue()))
                .collect(Collectors.toList());
    }
}
