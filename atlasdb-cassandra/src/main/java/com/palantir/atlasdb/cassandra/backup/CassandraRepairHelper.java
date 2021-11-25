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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.TargetedSweepTables;
import com.palantir.atlasdb.timelock.api.Namespace;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

public class CassandraRepairHelper {
    private static final String COORDINATION = AtlasDbConstants.COORDINATION_TABLE.getTableName();
    private static final Set<String> TABLES_TO_REPAIR =
            Sets.union(ImmutableSet.of(COORDINATION), TargetedSweepTables.REPAIR_ON_RESTORE);

    private final Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory;
    private final Function<Namespace, KeyValueService> keyValueServiceFactory;

    public CassandraRepairHelper(
            Function<Namespace, CassandraKeyValueServiceConfig> keyValueServiceConfigFactory,
            Function<Namespace, KeyValueService> keyValueServiceFactory) {
        this.keyValueServiceConfigFactory = keyValueServiceConfigFactory;
        this.keyValueServiceFactory = keyValueServiceFactory;
    }

    public void repairInternalTables(
            Namespace namespace, Consumer<Map<InetSocketAddress, Set<TokenRange>>> repairTable) {
        KeyValueService kvs = keyValueServiceFactory.apply(namespace);
        kvs.getAllTableNames().stream()
                .map(TableReference::getTableName)
                .filter(TABLES_TO_REPAIR::contains)
                .map(tableName -> getRangesToRepair(namespace, tableName))
                .forEach(repairTable);
    }

    private Map<InetSocketAddress, Set<TokenRange>> getRangesToRepair(Namespace namespace, String tableName) {
        CassandraKeyValueServiceConfig config = keyValueServiceConfigFactory.apply(namespace);
        return CqlCluster.create(config).getTokenRanges(tableName);
    }
}
