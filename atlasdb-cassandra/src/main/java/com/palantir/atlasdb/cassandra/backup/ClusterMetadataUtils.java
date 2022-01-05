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

package com.palantir.atlasdb.cassandra.backup;

import static com.google.common.collect.ImmutableRangeSet.toImmutableRangeSet;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MoreCollectors;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

public final class ClusterMetadataUtils {
    private ClusterMetadataUtils() {
        // util class
    }

    public static TableMetadata getTableMetadata(CqlMetadata metadata, String keyspace, String table) {
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspace);
        Optional<TableMetadata> maybeTable = keyspaceMetadata.getTables().stream()
                .filter(tableMetadata -> tableMetadata.getName().equals(table))
                .collect(MoreCollectors.toOptional());
        return maybeTable.orElseThrow(() -> new SafeIllegalArgumentException(
                "Can't find table",
                SafeArg.of("keyspace", keyspace),
                LoggingArgs.tableRef("table", TableReference.fromString(table))));
    }

    /**
     * Returns a mapping of Node to token ranges its host contains, where every partition key in the specified
     * list is present in the token ranges for each replica.
     *
     * <p>For example there is a cluster that has keyspace K with a simple replication strategy and replication factor
     * 2, the ring of the cluster has a range of (0, 3], and there are 3 nodes A, B, and C, where:
     *
     * <ul>
     *   <li>A has replicas for K for token ranges (1, 2] and (2, 3]
     *   <li>B has replicas for K for token ranges (2, 3] and (0, 1]
     *   <li>C has replicas for K for token ranges (0, 1] and (1, 2]
     * </ul>
     *
     * <p>The value returned by calling getTokenMapping([A, B, C], metadata, K, [0.25, 0.5, 2.5]) would be: <br>
     * { A : set[ range(2, 3] ], B : set[ range(0, 1],  range(2, 3]] }, C : set[ range(0, 1] ] }
     *
     * @param nodeSet The Cassandra nodes whose replicas to check
     * @param metadata The Datastax driver metadata from the Cassandra cluster
     * @param keyspace The keyspace containing the partition keys we want to map
     * @param partitionKeyTokens The Cassandra tokens for the partition keys we want to map
     * @return Mapping of Node to token ranges its host contains, where every partition key in the specified
     * list is present in one of the token ranges on one host
     */
    @SuppressWarnings("ReverseDnsLookup")
    public static Map<InetSocketAddress, RangeSet<LightweightOppToken>> getTokenMapping(
            Collection<InetSocketAddress> nodeSet,
            CqlMetadata metadata,
            String keyspace,
            Set<LightweightOppToken> partitionKeyTokens) {

        Map<String, InetSocketAddress> nodeMetadataHostMap =
                KeyedStream.of(nodeSet).mapKeys(InetSocketAddress::getHostName).collectToMap();
        Map<Host, RangeSet<LightweightOppToken>> hostToTokenRangeMap =
                getTokenMappingForPartitionKeys(metadata, keyspace, partitionKeyTokens);

        return KeyedStream.stream(hostToTokenRangeMap)
                .mapKeys(host -> lookUpAddress(nodeMetadataHostMap, host))
                .collectToMap();
    }

    @SuppressWarnings("ReverseDnsLookup")
    private static InetSocketAddress lookUpAddress(Map<String, InetSocketAddress> nodeMetadataHostMap, Host host) {
        String hostname = host.getEndPoint().resolve().getHostName();
        Preconditions.checkArgument(
                nodeMetadataHostMap.containsKey(hostname),
                "Did not find corresponding Node to run repair",
                SafeArg.of("hostname", hostname),
                SafeArg.of("availableHosts", nodeMetadataHostMap.keySet()),
                SafeArg.of("host", host.getEndPoint().resolve()));
        return nodeMetadataHostMap.get(hostname);
    }

    private static Map<Host, RangeSet<LightweightOppToken>> getTokenMappingForPartitionKeys(
            CqlMetadata metadata, String keyspace, Set<LightweightOppToken> partitionKeyTokens) {
        Set<Range<LightweightOppToken>> tokenRanges = metadata.getTokenRanges();
        SortedMap<LightweightOppToken, Range<LightweightOppToken>> tokenRangesByEnd =
                KeyedStream.of(tokenRanges).mapKeys(Range::upperEndpoint).collectTo(TreeMap::new);
        RangeSet<LightweightOppToken> ranges = getMinimalSetOfRangesForTokens(partitionKeyTokens, tokenRangesByEnd);
        Multimap<Host, Range<LightweightOppToken>> tokenMapping = ArrayListMultimap.create();
        ranges.asRanges().forEach(range -> {
            List<Host> hosts = ImmutableList.copyOf(metadata.getReplicas(keyspace, range));
            if (hosts.isEmpty()) {
                throw new SafeIllegalStateException(
                        "Failed to find any replicas of token range for repair",
                        SafeArg.of("tokenRange", range.toString()),
                        SafeArg.of("keyspace", quotedKeyspace(keyspace)));
            }
            hosts.forEach(host -> tokenMapping.put(host, range));
        });
        return KeyedStream.stream(tokenMapping.asMap())
                .map(Collection::stream)
                .map(stream -> (RangeSet<LightweightOppToken>) stream.collect(toImmutableRangeSet()))
                .collectToMap();
    }

    // VisibleForTesting
    public static RangeSet<LightweightOppToken> getMinimalSetOfRangesForTokens(
            Set<LightweightOppToken> partitionKeyTokens,
            SortedMap<LightweightOppToken, Range<LightweightOppToken>> tokenRangesByEnd) {
        Map<LightweightOppToken, Range<LightweightOppToken>> tokenRangesByStartToken = new HashMap<>();
        for (LightweightOppToken token : partitionKeyTokens) {
            Range<LightweightOppToken> minimalTokenRange = findTokenRange(token, tokenRangesByEnd);
            tokenRangesByStartToken.merge(
                    minimalTokenRange.lowerEndpoint(), minimalTokenRange, ClusterMetadataUtils::findLatestEndingRange);
        }
        return tokenRangesByStartToken.values().stream().collect(toImmutableRangeSet());
    }

    private static Range<LightweightOppToken> findTokenRange(
            LightweightOppToken token, SortedMap<LightweightOppToken, Range<LightweightOppToken>> tokenRangesByEnd) {
        if (tokenRangesByEnd.containsKey(token)) {
            return tokenRangesByEnd.get(token);
        } else if (!tokenRangesByEnd.headMap(token).isEmpty()) {
            return Range.closed(tokenRangesByEnd.headMap(token).lastKey(), token);
        } else {
            // Confirm that the first entry in the sorted map is the wraparound range
            Range<LightweightOppToken> firstTokenRange = tokenRangesByEnd.get(tokenRangesByEnd.firstKey());
            Preconditions.checkState(
                    // TODO(gs): port wraparound
                    true,
                    //                    firstTokenRange.isWrappedAround(),
                    "Failed to identify wraparound token range",
                    SafeArg.of("firstTokenRange", firstTokenRange),
                    SafeArg.of("token", token));
            return Range.closed(firstTokenRange.lowerEndpoint(), token);
        }
    }

    private static String quotedKeyspace(String keyspaceName) {
        return "\"" + keyspaceName + "\"";
    }

    // VisibleForTesting
    public static Range<LightweightOppToken> findLatestEndingRange(
            Range<LightweightOppToken> range1, Range<LightweightOppToken> range2) {
        Preconditions.checkArgument(
                range1.lowerEndpoint().equals(range2.lowerEndpoint()),
                "Expects token ranges to have the same start token",
                SafeArg.of("range1", range1),
                SafeArg.of("range2", range2));

        // TODO(gs): wraparound
        //        Set<TokenRange> wrapAroundTokenRanges =
        //                Stream.of(range1, range2).filter(TokenRange::isWrappedAround).collect(Collectors.toSet());
        //
        //        // If any token ranges are wraparound ranges, the non-wraparound ranges cannot possibly be the longest
        // range
        //        if (wrapAroundTokenRanges.size() == 1) {
        //            return wrapAroundTokenRanges.iterator().next();
        //        }

        if (range1.contains(range2.upperEndpoint())) {
            return range1;
        } else if (range2.contains(range1.upperEndpoint())) {
            return range2;
        } else {
            throw new SafeIllegalArgumentException(
                    "Cannot find max token range",
                    SafeArg.of("tokenRange1", range1),
                    SafeArg.of("tokenRange2", range2));
        }
    }
}
