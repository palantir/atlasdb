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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.base.Throwables;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class CassandraTableDropper {
    private static final Logger log = LoggerFactory.getLogger(CassandraTableDropper.class);
    private final CassandraKeyValueServiceConfig config;
    private final CassandraClientPool clientPool;
    private final CassandraTableMetadata cassandraTableMetadata;
    private final CassandraTableTruncator cassandraTableTruncator;

    CassandraTableDropper(CassandraKeyValueServiceConfig config, CassandraClientPool clientPool,
            CassandraTableMetadata cassandraTableMetadata, CassandraTableTruncator cassandraTableTruncator) {
        this.config = config;
        this.clientPool = clientPool;
        this.cassandraTableMetadata = cassandraTableMetadata;
        this.cassandraTableTruncator = cassandraTableTruncator;
    }

    void dropTables(final Set<TableReference> tablesToDrop) {
        try {
            clientPool.runWithRetry(client -> {
                KsDef ks = client.describe_keyspace(config.getKeyspaceOrThrow());
                Set<TableReference> existingTables = Sets.newHashSet();

                existingTables.addAll(ks.getCf_defs().stream()
                        .map(CassandraKeyValueServices::tableReferenceFromCfDef)
                        .collect(Collectors.toList()));

                for (TableReference table : tablesToDrop) {
                    CassandraVerifier.sanityCheckTableName(table);
                    if (existingTables.contains(table)) {
                        CassandraKeyValueServices.runWithWaitingForSchemas(
                                () -> truncateThenDrop(table, client), config, client,
                                "dropping the column family for table " + table + " in a call to drop tables");
                        cassandraTableMetadata.deleteAllMetadataRowsForTable(table);
                    } else {
                        log.warn("Ignored call to drop a table ({}) that did not exist.", LoggingArgs.tableRef(table));
                    }
                }
                return null;
            });
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private void truncateThenDrop(TableReference tableRef, CassandraClient client) throws TException {
        cassandraTableTruncator.runTruncateOnClient(ImmutableSet.of(tableRef), client);
        client.system_drop_column_family(CassandraKeyValueServiceImpl.internalTableName(tableRef));
    }
}
