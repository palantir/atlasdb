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

import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.Blacklist;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraClientPoolMetrics;
import com.palantir.atlasdb.keyvalue.cassandra.pool.CassandraService;
import com.palantir.atlasdb.schema.TargetedSweepTables;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.streams.KeyedStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CassandraRepairHelper {
    private static final String COORDINATION = AtlasDbConstants.COORDINATION_TABLE.getTableName();
    private static final Set<String> TABLES_TO_REPAIR =
            Sets.union(ImmutableSet.of(COORDINATION), TargetedSweepTables.REPAIR_ON_RESTORE);

    private final MetricsManager metricsManager;
    private final Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory;
    private final Function<Namespace, KeyValueService> keyValueServiceFactory;

    public CassandraRepairHelper(
            MetricsManager metricsManager,
            Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
        this.metricsManager = metricsManager;
        this.keyValueServiceConfigFactory = keyValueServiceConfigFactory;
        this.keyValueServiceFactory = keyValueServiceFactory;
    }

    public void repairInternalTables(
            Namespace namespace, Consumer<Map<InetSocketAddress, Set<LightweightOppTokenRange>>> repairTable) {
        KeyValueService kvs = keyValueServiceFactory.apply(namespace);
        kvs.getAllTableNames().stream()
                .map(TableReference::getTableName)
                .filter(TABLES_TO_REPAIR::contains)
                .map(tableName -> getRangesToRepair(namespace, tableName))
                .forEach(repairTable);
    }

    // TODO(gs): test: use both CQL and CassandraService flavours, and compare results on 3-node/RF2 cluster
    // VisibleForTesting
    public Map<InetSocketAddress, Set<LightweightOppTokenRange>> getRangesToRepair(
            Namespace namespace, String _tableName) {
        CassandraKeyValueServiceConfig config = keyValueServiceConfigFactory.apply(namespace);
        Blacklist blacklist = new Blacklist(config);
        CassandraService cassandraService =
                new CassandraService(metricsManager, config, blacklist, new CassandraClientPoolMetrics(metricsManager));
        @SuppressWarnings("UnstableApiUsage")
        RangeMap<LightweightOppToken, List<InetSocketAddress>> tokenMap = cassandraService.getTokenMap();
        return invert(tokenMap);
    }

    @SuppressWarnings("UnstableApiUsage")
    private Map<InetSocketAddress, Set<LightweightOppTokenRange>> invert(
            RangeMap<LightweightOppToken, List<InetSocketAddress>> tokenMap) {
        Map<InetSocketAddress, Set<LightweightOppTokenRange>> invertedMap = new HashMap<>();
        tokenMap.asMapOfRanges()
                .forEach((range, addresses) -> addresses.forEach(addr -> {
                    Set<LightweightOppTokenRange> existingRanges = invertedMap.getOrDefault(addr, new HashSet<>());
                    existingRanges.add(toTokenRange(range));
                    invertedMap.put(addr, existingRanges);
                }));

        return invertedMap;
    }

    // TODO(gs): handle wrap-around?
    private LightweightOppTokenRange toTokenRange(Range<LightweightOppToken> range) {
        return LightweightOppTokenRange.builder()
                .left(range.lowerEndpoint())
                .right(range.upperEndpoint())
                .build();
    }

    @VisibleForTesting
    Map<InetSocketAddress, Set<TokenRange>> getRangesToRepairCql(Namespace namespace, String tableName) {
        CassandraKeyValueServiceConfig config = keyValueServiceConfigFactory.apply(namespace);
        return CqlCluster.create(config).getTokenRanges(tableName);
    }

    Map<InetSocketAddress, Set<LightweightOppTokenRange>> getLwRangesToRepairCql(
            Namespace namespace, String tableName) {
        Map<InetSocketAddress, Set<TokenRange>> tokenRanges = getRangesToRepairCql(namespace, tableName);
        return KeyedStream.stream(tokenRanges).map(this::makeLightweight).collectToMap();
    }

    private Set<LightweightOppTokenRange> makeLightweight(Set<TokenRange> tokenRanges) {
        return tokenRanges.stream()
                .map(this::makeLightweight)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private Set<LightweightOppTokenRange> makeLightweight(TokenRange tokenRange) {
        // TODO(gs): copied (almost) from CassandraService
        // TODO(gs): serialise?
        LightweightOppToken startToken = new LightweightOppToken(
                BaseEncoding.base16().decode(tokenRange.getStart().toString().toUpperCase()));
        LightweightOppToken endToken = new LightweightOppToken(
                BaseEncoding.base16().decode(tokenRange.getEnd().toString().toUpperCase()));

        if (startToken.compareTo(endToken) <= 0) {
            return ImmutableSet.of(LightweightOppTokenRange.builder()
                    .left(startToken)
                    .right(endToken)
                    .build());
        } else {
            // Handle wrap-around
            ByteBuffer minValue = ByteBuffer.allocate(0);
            // TODO(gs): extract method
            LightweightOppToken minToken = new LightweightOppToken(
                    BaseEncoding.base16().decode(Bytes.toHexString(minValue).toUpperCase()));
            LightweightOppTokenRange greaterThan = LightweightOppTokenRange.builder()
                    .left(startToken)
                    .right(minToken)
                    .build();
            LightweightOppTokenRange atMost = LightweightOppTokenRange.builder()
                    .left(minToken)
                    .right(endToken)
                    .build();

            return ImmutableSet.of(greaterThan, atMost);
        }
    }
}
