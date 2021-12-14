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

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
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
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ClusterMetadataUtils {
    private ClusterMetadataUtils() {
        // util class
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
    public static Map<InetSocketAddress, Set<TokenRange>> getTokenMapping(
            Collection<InetSocketAddress> nodeSet, Metadata metadata, String keyspace, Set<Token> partitionKeyTokens) {

        Map<String, InetSocketAddress> nodeMetadataHostMap =
                KeyedStream.of(nodeSet).mapKeys(InetSocketAddress::getHostName).collectToMap();
        Map<Host, List<TokenRange>> hostToTokenRangeMap =
                getTokenMappingForPartitionKeys(metadata, keyspace, partitionKeyTokens);

        return KeyedStream.stream(hostToTokenRangeMap)
                .mapKeys(host -> lookUpAddress(nodeMetadataHostMap, host))
                .map(List::stream)
                .map(stream -> stream.collect(Collectors.toSet()))
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

    private static Map<Host, List<TokenRange>> getTokenMappingForPartitionKeys(
            Metadata metadata, String keyspace, Set<Token> partitionKeyTokens) {
        Set<TokenRange> tokenRanges = metadata.getTokenRanges();
        SortedMap<Token, TokenRange> tokenRangesByEnd =
                KeyedStream.of(tokenRanges).mapKeys(TokenRange::getEnd).collectTo(TreeMap::new);
        Set<TokenRange> ranges = getMinimalSetOfRangesForTokens(metadata, partitionKeyTokens, tokenRangesByEnd);
        Multimap<Host, TokenRange> tokenMapping = ArrayListMultimap.create();
        ranges.forEach(range -> {
            List<Host> hosts = ImmutableList.copyOf(metadata.getReplicas(quotedKeyspace(keyspace), range));
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
                .map(stream -> stream.collect(Collectors.toList()))
                .collectToMap();
    }

    // VisibleForTesting
    public static Set<TokenRange> getMinimalSetOfRangesForTokens(
            Metadata metadata, Set<Token> partitionKeyTokens, SortedMap<Token, TokenRange> tokenRangesByEnd) {
        Map<Token, TokenRange> tokenRangesByStartToken = new HashMap<>();
        for (Token token : partitionKeyTokens) {
            TokenRange minimalTokenRange = findTokenRange(metadata, token, tokenRangesByEnd);
            tokenRangesByStartToken.merge(
                    minimalTokenRange.getStart(), minimalTokenRange, ClusterMetadataUtils::findLatestEndingRange);
        }
        return ImmutableSet.copyOf(tokenRangesByStartToken.values());
    }

    private static TokenRange findTokenRange(
            Metadata metadata, Token token, SortedMap<Token, TokenRange> tokenRangesByEnd) {
        if (tokenRangesByEnd.containsKey(token)) {
            return tokenRangesByEnd.get(token);
        } else if (!tokenRangesByEnd.headMap(token).isEmpty()) {
            return metadata.newTokenRange(tokenRangesByEnd.headMap(token).lastKey(), token);
        } else {
            // Confirm that the first entry in the sorted map is the wraparound range
            TokenRange firstTokenRange = tokenRangesByEnd.get(tokenRangesByEnd.firstKey());
            Preconditions.checkState(
                    firstTokenRange.isWrappedAround(),
                    "Failed to identify wraparound token range",
                    SafeArg.of("firstTokenRange", firstTokenRange),
                    SafeArg.of("token", token));
            return metadata.newTokenRange(firstTokenRange.getStart(), token);
        }
    }

    private static String quotedKeyspace(String keyspaceName) {
        return "\"" + keyspaceName + "\"";
    }

    // VisibleForTesting
    public static TokenRange findLatestEndingRange(TokenRange range1, TokenRange range2) {
        Preconditions.checkArgument(
                range1.getStart().equals(range2.getStart()),
                "Expects token ranges to have the same start token",
                SafeArg.of("range1", range1),
                SafeArg.of("range2", range2));

        Set<TokenRange> wrapAroundTokenRanges =
                Stream.of(range1, range2).filter(TokenRange::isWrappedAround).collect(Collectors.toSet());

        // If any token ranges are wraparound ranges, the non-wraparound ranges cannot possibly be the longest range
        if (wrapAroundTokenRanges.size() == 1) {
            return wrapAroundTokenRanges.iterator().next();
        }

        if (range1.contains(range2.getEnd())) {
            return range1;
        } else if (range2.contains(range1.getEnd())) {
            return range2;
        } else {
            throw new SafeIllegalArgumentException(
                    "Cannot find max token range",
                    SafeArg.of("tokenRange1", range1),
                    SafeArg.of("tokenRange2", range2));
        }
    }
}
