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

import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraServer;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.cassandra.thrift.TokenRange;
import org.immutables.value.Value;

public final class CassandraLogHelper {
    private CassandraLogHelper() {
        // Utility class.
    }

    public static HostAndIpAddress host(InetSocketAddress host) {
        return HostAndIpAddress.fromAddress(host);
    }

    static Collection<String> collectionOfHosts(Collection<CassandraServer> hosts) {
        return hosts.stream().map(CassandraServer::cassandraHostName).collect(Collectors.toSet());
    }

    static List<String> tokenRangesToServer(Multimap<Set<TokenRange>, CassandraServer> tokenRangesToHost) {
        return tokenRangesToHost.entries().stream()
                .map(entry -> "host " + entry.getValue().cassandraHostName() + " has range "
                        + entry.getKey().toString())
                .collect(Collectors.toList());
    }

    public static List<String> tokenMap(RangeMap<LightweightOppToken, List<CassandraServer>> tokenMap) {
        return tokenMap.asMapOfRanges().entrySet().stream()
                .map(rangeListToHostEntry -> String.format(
                        "range from %s to %s is on host %s",
                        getLowerEndpoint(rangeListToHostEntry.getKey()),
                        getUpperEndpoint(rangeListToHostEntry.getKey()),
                        CassandraLogHelper.collectionOfHosts(rangeListToHostEntry.getValue())))
                .collect(Collectors.toList());
    }

    private static String getLowerEndpoint(Range<LightweightOppToken> range) {
        if (!range.hasLowerBound()) {
            return "(no lower bound)";
        }
        return range.lowerEndpoint().toString();
    }

    private static String getUpperEndpoint(Range<LightweightOppToken> range) {
        if (!range.hasUpperBound()) {
            return "(no upper bound)";
        }
        return range.upperEndpoint().toString();
    }

    @Value.Immutable
    interface HostAndIpAddress {
        String host();

        Optional<String> ipAddress();

        static HostAndIpAddress fromAddress(InetSocketAddress address) {
            return ImmutableHostAndIpAddress.builder()
                    .host(address.getHostString())
                    .ipAddress(Optional.ofNullable(address.getAddress()).map(InetAddress::getHostAddress))
                    .build();
        }
    }
}
