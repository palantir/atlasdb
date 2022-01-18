/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import com.google.common.collect.ImmutableMap;
import com.palantir.common.streams.KeyedStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

public class CassandraPoolReaper {
    private volatile Map<InetSocketAddress, Integer> consecutiveRemovalRequests;
    private final int requiredConsecutiveRequestsBeforeRemoval;

    public CassandraPoolReaper(int requiredConsecutiveRequestsBeforeRemoval) {
        this.consecutiveRemovalRequests = ImmutableMap.of();
        this.requiredConsecutiveRequestsBeforeRemoval = requiredConsecutiveRequestsBeforeRemoval;
    }

    // The pool reaper is only called by one thread at a time, just that which thread that is may differ.
    @SuppressWarnings("NonAtomicVolatileUpdate")
    public Set<InetSocketAddress> computeServersToRemove(Set<InetSocketAddress> request) {
        consecutiveRemovalRequests = KeyedStream.of(request)
                .map(address -> consecutiveRemovalRequests.getOrDefault(address, 0) + 1)
                .collectToMap();

        return consecutiveRemovalRequests.entrySet().stream()
                .filter(entry -> entry.getValue() >= requiredConsecutiveRequestsBeforeRemoval)
                .map(Entry::getKey)
                .collect(Collectors.toSet());
    }
}
