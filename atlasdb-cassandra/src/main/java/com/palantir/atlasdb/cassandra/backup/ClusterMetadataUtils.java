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

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MoreCollectors;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.timelock.api.Namespace;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ClusterMetadataUtils {
    private ClusterMetadataUtils() {
        // util class
    }

    public static TableMetadata getTableMetadata(CqlMetadata metadata, Namespace namespace, String table) {
        KeyspaceMetadata keyspaceMetadata = metadata.getKeyspaceMetadata(namespace);
        Optional<TableMetadata> maybeTable = keyspaceMetadata.getTables().stream()
                .filter(tableMetadata -> tableMetadata.getName().equals(table))
                .collect(MoreCollectors.toOptional());
        return maybeTable.orElseThrow(() -> new SafeIllegalArgumentException(
                "Can't find table",
                SafeArg.of("keyspace", namespace),
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
     * @param namespace The namespace containing the partition keys we want to map
     * @param partitionKeyTokens The Cassandra tokens for the partition keys we want to map
     * @return Mapping of Node to token ranges its host contains, where every partition key in the specified
     * list is present in one of the token ranges on one host
     */
    @SuppressWarnings("ReverseDnsLookup")
    public static Map<InetSocketAddress, RangeSet<LightweightOppToken>> getTokenMapping(
            Collection<InetSocketAddress> nodeSet,
            CqlMetadata metadata,
            Namespace namespace,
            Set<LightweightOppToken> partitionKeyTokens) {

        Map<String, InetSocketAddress> nodeMetadataHostMap =
                KeyedStream.of(nodeSet).mapKeys(InetSocketAddress::getHostName).collectToMap();
        Map<InetSocketAddress, RangeSet<LightweightOppToken>> hostToTokenRangeMap =
                getTokenMappingForPartitionKeys(metadata, namespace, partitionKeyTokens);

        return KeyedStream.stream(hostToTokenRangeMap)
                .mapKeys(host -> lookUpAddress(nodeMetadataHostMap, host))
                .collectToMap();
    }

    @SuppressWarnings("ReverseDnsLookup")
    private static InetSocketAddress lookUpAddress(
            Map<String, InetSocketAddress> nodeMetadataHostMap, InetSocketAddress host) {
        String hostname = host.getHostName();
        Preconditions.checkArgument(
                nodeMetadataHostMap.containsKey(hostname),
                "Did not find corresponding Node to run repair",
                SafeArg.of("hostname", hostname),
                SafeArg.of("availableHosts", nodeMetadataHostMap.keySet()),
                SafeArg.of("host", host));
        return nodeMetadataHostMap.get(hostname);
    }

    private static Map<InetSocketAddress, RangeSet<LightweightOppToken>> getTokenMappingForPartitionKeys(
            CqlMetadata metadata, Namespace namespace, Set<LightweightOppToken> partitionKeyTokens) {
        Set<Range<LightweightOppToken>> tokenRanges = metadata.getTokenRanges();
        SortedMap<LightweightOppToken, Range<LightweightOppToken>> tokenRangesByStart = KeyedStream.of(tokenRanges)
                .mapKeys(LightweightOppToken::getLowerExclusive)
                .collectTo(TreeMap::new);
        Set<Range<LightweightOppToken>> ranges = getMinimalSetOfRangesForTokens(partitionKeyTokens, tokenRangesByStart);
        Multimap<InetSocketAddress, Range<LightweightOppToken>> tokenMapping = ArrayListMultimap.create();
        ranges.forEach(range -> {
            List<InetSocketAddress> hosts = ImmutableList.copyOf(metadata.getReplicas(namespace, range));
            if (hosts.isEmpty()) {
                throw new SafeIllegalStateException(
                        "Failed to find any replicas of token range for repair",
                        SafeArg.of("tokenRange", range.toString()),
                        SafeArg.of("keyspace", quotedNamespace(namespace)));
            }
            hosts.forEach(host -> tokenMapping.put(host, range));
        });
        return KeyedStream.stream(tokenMapping.asMap())
                .map(Collection::stream)
                .map(stream -> (RangeSet<LightweightOppToken>) stream.collect(toImmutableRangeSet()))
                .collectToMap();
    }

    @VisibleForTesting
    // Needs to be Set<Range<>> as we don't want to merge ranges yet
    static Set<Range<LightweightOppToken>> getMinimalSetOfRangesForTokens(
            Set<LightweightOppToken> partitionKeyTokens,
            SortedMap<LightweightOppToken, Range<LightweightOppToken>> tokenRangesByStart) {
        Map<LightweightOppToken, Range<LightweightOppToken>> tokenRangesByStartToken = new HashMap<>();
        for (LightweightOppToken token : partitionKeyTokens) {
            Range<LightweightOppToken> minimalTokenRange = findTokenRange(token, tokenRangesByStart);
            tokenRangesByStartToken.merge(
                    LightweightOppToken.getLowerExclusive(minimalTokenRange),
                    minimalTokenRange,
                    ClusterMetadataUtils::findLatestEndingRange);

            if (!minimalTokenRange.hasLowerBound()) {
                // handle wraparound
                Optional<Range<LightweightOppToken>> wraparoundRange = tokenRangesByStart.values().stream()
                        .filter(Predicate.not(Range::hasUpperBound))
                        .findFirst();
                wraparoundRange.ifPresent(range -> tokenRangesByStartToken.merge(
                        LightweightOppToken.getLowerExclusive(range),
                        range,
                        ClusterMetadataUtils::findLatestEndingRange));
            }
        }
        return ImmutableSet.copyOf(tokenRangesByStartToken.values());
    }

    private static Range<LightweightOppToken> findTokenRange(
            LightweightOppToken token, SortedMap<LightweightOppToken, Range<LightweightOppToken>> tokenRangesByStart) {
        if (tokenRangesByStart.headMap(token).isEmpty()) {
            // Shouldn't happen, as tokenRangesByStart should include the whole token ring, including some range
            // starting with minus infinity.
            throw new SafeIllegalStateException(
                    "Unable to find token range for token, as a full token ring was not supplied",
                    SafeArg.of("token", token),
                    SafeArg.of("tokenRangesByStart", tokenRangesByStart));
        }

        LightweightOppToken lowerBound = tokenRangesByStart.headMap(token).lastKey();
        if (lowerBound.isEmpty()) {
            return Range.atMost(token);
        } else {
            return Range.openClosed(lowerBound, token);
        }
    }

    private static String quotedNamespace(Namespace namespace) {
        return "\"" + namespace.value() + "\"";
    }

    // VisibleForTesting
    public static Range<LightweightOppToken> findLatestEndingRange(
            Range<LightweightOppToken> range1, Range<LightweightOppToken> range2) {
        Preconditions.checkArgument(
                LightweightOppToken.getLowerExclusive(range1).equals(LightweightOppToken.getLowerExclusive(range2)),
                "Expects token ranges to have the same start token",
                SafeArg.of("range1", range1),
                SafeArg.of("range2", range2));

        Set<Range<LightweightOppToken>> wrapAroundTokenRanges = Stream.of(range1, range2)
                .filter(Predicate.not(Range::hasUpperBound))
                .collect(Collectors.toSet());
        // If any token ranges are wraparound ranges, the non-wraparound ranges cannot possibly be the longest range
        if (wrapAroundTokenRanges.size() == 1) {
            return wrapAroundTokenRanges.iterator().next();
        }

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
