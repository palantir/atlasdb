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
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.palantir.logsafe.SafeArg;

public class Blacklist {
    private static final Logger log = LoggerFactory.getLogger(CassandraClientPool.class);

    private Map<InetSocketAddress, Long> blacklistedHosts = Maps.newConcurrentMap();

    public Map<InetSocketAddress, Long> getBlacklistedHosts() {
        return blacklistedHosts;
    }

    // TODO (gsheasby): Why did we blacklist this host?
    public void add(InetSocketAddress badHost) {
        blacklistedHosts.put(badHost, System.currentTimeMillis());
        log.warn("Blacklisted host '{}'", SafeArg.of("badHost", CassandraLogHelper.host(badHost)));
    }

    public void addAll(Set<InetSocketAddress> hosts) {
        hosts.forEach(this::add);
    }

    public boolean contains(InetSocketAddress specifiedHost) {
        return blacklistedHosts.containsKey(specifiedHost);
    }

    public void remove(InetSocketAddress removedServerAddress) {
        blacklistedHosts.remove(removedServerAddress);
    }

    public void removeAll() {
        blacklistedHosts.clear();
    }

    public int size() {
        return blacklistedHosts.size();
    }
}
