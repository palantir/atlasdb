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

package com.palantir.atlasdb.keyvalue.cassandra.pool;

import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.immutables.value.Value;

@Value.Immutable
@SuppressWarnings("UnstableApiUsage")
public interface TokenRanges {
    RangeMap<LightweightOppToken, ? extends Set<CassandraServer>> tokenMap();

    Map<CassandraServer, String> hostToAvailabilityZone();

    @Value.Derived
    default Map<String, List<CassandraServer>> availabilityZoneToHosts() {
        return hostToAvailabilityZone().entrySet().stream()
                .sorted(Entry.comparingByKey(Comparator.comparing(CassandraServer::cassandraHostName)))
                .collect(
                        Collectors.groupingBy(Entry::getValue, Collectors.mapping(Entry::getKey, Collectors.toList())));
    }
}
