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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class BlacklistManager {
    private static final Logger log = LoggerFactory.getLogger(CassandraClientPool.class);

    private final CassandraKeyValueServiceConfig config;
    private Map<InetSocketAddress, Long> blacklist;

    BlacklistManager(CassandraKeyValueServiceConfig config) {
        this.config = config;
        this.blacklist = Maps.newConcurrentMap();
    }

    public void checkAndUpdateBlacklist(Map<InetSocketAddress, CassandraClientPoolingContainer> pools) {
        // Check blacklist and re-integrate or continue to wait as necessary
        for (Map.Entry<InetSocketAddress, Long> blacklistedEntry : blacklist.entrySet()) {
            if (coolOffPeriodExpired(blacklistedEntry)) {
                InetSocketAddress host = blacklistedEntry.getKey();
                if (isHostHealthy(pools.get(host))) {
                    blacklist.remove(host);
                    log.info("Added host {} back into the pool after a waiting period and successful health check.",
                            SafeArg.of("host", CassandraLogHelper.host(host)));
                }
            }
        }
    }

    private boolean coolOffPeriodExpired(Map.Entry<InetSocketAddress, Long> blacklistedEntry) {
        long backoffTimeMillis = TimeUnit.SECONDS.toMillis(config.unresponsiveHostBackoffTimeSeconds());
        return blacklistedEntry.getValue() + backoffTimeMillis < System.currentTimeMillis();
    }

    private boolean isHostHealthy(CassandraClientPoolingContainer container) {
        try {
            container.runWithPooledResource(CassandraUtils.getDescribeRing(config));
            container.runWithPooledResource(CassandraUtils.getValidatePartitioner(config));
            return true;
        } catch (Exception e) {
            log.warn("We tried to blacklist {} back into the pool, but got an exception"
                            + " that caused us to distrust this host further. Exception message was: {} : {}",
                    SafeArg.of("host", CassandraLogHelper.host(container.getHost())),
                    SafeArg.of("exceptionClass", e.getClass().getCanonicalName()),
                    UnsafeArg.of("exceptionMessage", e.getMessage()));
            return false;
        }
    }

    public Set<InetSocketAddress> filterBlacklistedHostsFrom(Collection<InetSocketAddress> potentialHosts) {
        return Sets.difference(ImmutableSet.copyOf(potentialHosts), blacklist.keySet());
    }

    public boolean isBlacklisted(InetSocketAddress host) {
        return blacklist.containsKey(host);
    }

    public void blacklist(InetSocketAddress host) {
        blacklist.put(host, System.currentTimeMillis());
        log.warn("Blacklisted host '{}'", SafeArg.of("badHost", CassandraLogHelper.host(host)));
    }

    public void blacklistAll(Set<InetSocketAddress> hosts) {
        hosts.forEach(this::blacklist);
    }

    public void unblacklist(InetSocketAddress host) {
        blacklist.remove(host);
    }

    public void unblacklistAll() {
        blacklist.clear();
    }

    public int size() {
        return blacklist.size();
    }

    public String describeBlacklist() {
        return blacklist.keySet().toString();
    }

    public List<String> blacklistDetails() {
        return blacklist.entrySet().stream()
                .map(blacklistedHostToBlacklistTime -> String.format("host: %s was blacklisted at %s",
                        CassandraLogHelper.host(blacklistedHostToBlacklistTime.getKey()),
                        blacklistedHostToBlacklistTime.getValue().longValue()))
                .collect(Collectors.toList());

    }
}
