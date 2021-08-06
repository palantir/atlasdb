/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.routing;

import com.palantir.atlasdb.keyvalue.cassandra.Blacklist;
import java.net.InetSocketAddress;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlacklistHostFilter implements HostFilter {

    private final Blacklist blacklist;

    private static final Logger log = LoggerFactory.getLogger(BlacklistHostFilter.class);

    public BlacklistHostFilter(Blacklist blacklist) {
        this.blacklist = blacklist;
    }

    @Override
    public Set<InetSocketAddress> filter(Set<InetSocketAddress> desiredHosts) {
        Set<InetSocketAddress> filteredHosts = blacklist.filterBlacklistedHostsFrom(desiredHosts);

        if (!filteredHosts.isEmpty()) {
            log.info("There are no known live hosts in the connection pool matching the predicate. We're choosing"
                    + " one at random in a last-ditch attempt at forward progress.");
            return filteredHosts;
        }

        return desiredHosts;
    }
}
