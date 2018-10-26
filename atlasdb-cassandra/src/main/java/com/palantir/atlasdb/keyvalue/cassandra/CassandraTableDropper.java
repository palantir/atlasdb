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

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import com.palantir.common.exception.AtlasDbDependencyException;

class CassandraTableDropper {
    private static final Logger log = LoggerFactory.getLogger(CassandraTableDropper.class);
    private CassandraKeyValueServiceConfig config;
    private CassandraClientPool clientPool;
    private CellValuePutter cellValuePutter;
    private WrappingQueryRunner wrappingQueryRunner;
    private ConsistencyLevel deleteConsistency;

    CassandraTableDropper(CassandraKeyValueServiceConfig config,
            CassandraClientPool clientPool,
            CellValuePutter cellValuePutter,
            WrappingQueryRunner wrappingQueryRunner,
            ConsistencyLevel deleteConsistency) {
        this.config = config;
        this.clientPool = clientPool;
        this.cellValuePutter = cellValuePutter;
        this.wrappingQueryRunner = wrappingQueryRunner;
        this.deleteConsistency = deleteConsistency;
    }

    void dropTables(final Set<TableReference> tablesToDrop) {
        try {
            clientPool.runWithRetry(
                    (FunctionCheckedException<CassandraClient, Void, Exception>) client -> {
                        CassandraKeyValueServices.waitForSchemaAgreementOnAllNodes(config, client, "before dropping the"
                                + " column family for tables " + tablesToDrop + " in a call to drop tables");
                        KsDef ks = client.describe_keyspace(
                                config.getKeyspaceOrThrow());
                        Set<TableReference> existingTables = Sets.newHashSet();

                        existingTables.addAll(ks.getCf_defs().stream()
                                .map(CassandraKeyValueServices::tableReferenceFromCfDef)
                                .collect(Collectors.toList()));

                        for (TableReference table : tablesToDrop) {
                            CassandraVerifier.sanityCheckTableName(table);
                            if (existingTables.contains(table)) {
                                client.system_drop_column_family(
                                        CassandraKeyValueServiceImpl.internalTableName(table));
                                putMetadataWithoutChangingSettings(table,
                                        AtlasDbConstants.EMPTY_TABLE_METADATA);
                            } else {
                                log.warn("Ignored call to drop a table ({}) that did not exist.",
                                        LoggingArgs.tableRef(table));
                            }
                        }
                        CassandraKeyValueServices.waitForSchemaAgreementOnAllNodes(config, client, "after dropping the "
                                + "column family for tables " + tablesToDrop + " in a call to drop tables");
                        return null;
                    });
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private void putMetadataWithoutChangingSettings(final TableReference tableRef, final byte[] meta) {
        long ts = System.currentTimeMillis();

        try {
            cellValuePutter.put("put", AtlasDbConstants.DEFAULT_METADATA_TABLE,
                    KeyValueServices.toConstantTimestampValues(
                            ImmutableMap.of(CassandraKeyValueServices.getMetadataCell(tableRef), meta).entrySet(),
                            ts));
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }

        try {
            new CellRangeDeleter(clientPool, wrappingQueryRunner, deleteConsistency, no -> System.currentTimeMillis())
                    .deleteAllTimestamps(AtlasDbConstants.DEFAULT_METADATA_TABLE,
                            ImmutableMap.of(CassandraKeyValueServices.getMetadataCell(tableRef), ts), false);
        } catch (AtlasDbDependencyException e) {
            log.info("Failed to delete old table metadata for table {} because not all Cassandra nodes are up. However,"
                    + "the table has been dropped and the table metadata reflecting this has been successfully "
                    + "persisted.", LoggingArgs.tableRef(tableRef), e);
        }
    }
}
