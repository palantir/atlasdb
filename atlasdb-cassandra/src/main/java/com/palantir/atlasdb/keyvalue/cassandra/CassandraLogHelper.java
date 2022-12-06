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
import com.palantir.common.annotations.ImmutablesStyles.WeakInterningImmutablesStyle;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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
                .map(entry ->
                        "host " + entry.getValue().cassandraHostName() + " with proxy has range " + entry.getKey())
                .collect(Collectors.toList());
    }

    public static List<String> tokenMap(RangeMap<LightweightOppToken, ? extends Collection<CassandraServer>> tokenMap) {
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

    public static List<String> tokenRangeHashes(Set<TokenRange> tokenRanges) {
        return tokenRanges.stream()
                .map(range -> "(" + range.getStart_token().hashCode() + ", "
                        + range.getEnd_token().hashCode() + ")")
                .collect(Collectors.toList());
    }

    // Weakly intern instances as there should be a relatively small, generally fixed number of Cassandra servers.
    @Value.Immutable(lazyhash = true, intern = true, builder = false)
    @WeakInterningImmutablesStyle
    interface HostAndIpAddress {
        @Value.Parameter
        String host();

        @Nullable
        @Value.Parameter
        String ipAddress();

        static HostAndIpAddress fromAddress(InetSocketAddress address) {
            InetAddress inetAddress = address.getAddress();
            if (inetAddress != null) {
                return ImmutableHostAndIpAddress.of(address.getHostString(), inetAddress.getHostAddress());
            }
            return ImmutableHostAndIpAddress.of(address.getHostString(), /* unresolved IP */ null);
        }
    }
}
