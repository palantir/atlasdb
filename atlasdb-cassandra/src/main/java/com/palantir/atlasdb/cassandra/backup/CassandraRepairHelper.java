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

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.LightweightOppToken;
import com.palantir.atlasdb.keyvalue.impl.AbstractKeyValueService;
import com.palantir.atlasdb.schema.TargetedSweepTables;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.streams.KeyedStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CassandraRepairHelper {
    private static final Set<TableReference> TABLES_TO_REPAIR =
            Sets.union(ImmutableSet.of(AtlasDbConstants.COORDINATION_TABLE), TargetedSweepTables.REPAIR_ON_RESTORE);

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
                .filter(TABLES_TO_REPAIR::contains)
                .map(TableReference::getTableName)
                .map(tableName -> getRangesToRepair(namespace, tableName))
                .forEach(repairTable);
    }

    // VisibleForTesting
    public Map<InetSocketAddress, Set<LightweightOppTokenRange>> getRangesToRepair(
            Namespace namespace, String tableName) {
        Map<InetSocketAddress, Set<TokenRange>> tokenRanges = getTokenRangesToRepair(namespace, tableName);
        return KeyedStream.stream(tokenRanges).map(this::makeLightweight).collectToMap();
    }

    private Map<InetSocketAddress, Set<TokenRange>> getTokenRangesToRepair(Namespace namespace, String tableName) {
        CassandraKeyValueServiceConfig config = keyValueServiceConfigFactory.apply(namespace);
        String cassandraTableName = getCassandraTableName(namespace, tableName);
        return CqlCluster.create(config).getTokenRanges(cassandraTableName);
    }

    private String getCassandraTableName(Namespace namespace, String tableName) {
        TableReference tableRef = TableReference.create(toKvNamespace(namespace), tableName);
        return AbstractKeyValueService.internalTableName(tableRef);
    }

    private com.palantir.atlasdb.keyvalue.api.Namespace toKvNamespace(Namespace namespace) {
        return com.palantir.atlasdb.keyvalue.api.Namespace.create(namespace.get());
    }

    private Set<LightweightOppTokenRange> makeLightweight(Set<TokenRange> tokenRanges) {
        return tokenRanges.stream()
                .map(this::makeLightweight)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    private Set<LightweightOppTokenRange> makeLightweight(TokenRange tokenRange) {
        LightweightOppToken startToken = serialise(tokenRange.getStart());
        LightweightOppToken endToken = serialise(tokenRange.getEnd());

        if (startToken.compareTo(endToken) <= 0) {
            return ImmutableSet.of(LightweightOppTokenRange.builder()
                    .left(startToken)
                    .right(endToken)
                    .build());
        } else {
            // Handle wrap-around
            ByteBuffer minValue = ByteBuffer.allocate(0);
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

    private LightweightOppToken serialise(Token token) {
        ByteBuffer serializedToken = token.serialize(ProtocolVersion.V3);
        byte[] bytes = new byte[serializedToken.remaining()];
        serializedToken.get(bytes);
        return new LightweightOppToken(bytes);
    }
}
