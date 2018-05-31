/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.UnavailableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.InsufficientConsistencyException;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.impl.KeyValueServices;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.atlasdb.qos.ratelimit.QosAwareThrowables;
import com.palantir.common.base.FunctionCheckedException;

class CassandraTableDropper {
    private static final Logger log = LoggerFactory.getLogger(CassandraTableDropper.class);
    private CassandraKeyValueServiceConfig config;
    private CassandraClientPool clientPool;
    private CellLoader cellLoader;
    private CellValuePutter cellValuePutter;
    private WrappingQueryRunner wrappingQueryRunner;
    private ConsistencyLevel deleteConsistency;

    CassandraTableDropper(CassandraKeyValueServiceConfig config,
            CassandraClientPool clientPool,
            CellLoader cellLoader,
            CellValuePutter cellValuePutter,
            WrappingQueryRunner wrappingQueryRunner,
            ConsistencyLevel deleteConsistency) {
        this.config = config;
        this.clientPool = clientPool;
        this.cellLoader = cellLoader;
        this.cellValuePutter = cellValuePutter;
        this.wrappingQueryRunner = wrappingQueryRunner;
        this.deleteConsistency = deleteConsistency;
    }

    void dropTables(final Set<TableReference> tablesToDrop) {
        try {
            clientPool.runWithRetry(
                    (FunctionCheckedException<CassandraClient, Void, Exception>) client -> {
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
                        CassandraKeyValueServices.waitForSchemaVersions(
                                config,
                                client,
                                "(all tables in a call to dropTables)");
                        return null;
                    });
        } catch (UnavailableException e) {
            throw new InsufficientConsistencyException(
                    "Dropping tables requires all Cassandra nodes to be up and available.", e);
        } catch (Exception e) {
            throw QosAwareThrowables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(e);
        }
    }

    private void putMetadataWithoutChangingSettings(final TableReference tableRef, final byte[] meta) {
        long ts = System.currentTimeMillis();

        Multimap<Cell, Long> oldVersions = cellLoader.getAllTimestamps(AtlasDbConstants.DEFAULT_METADATA_TABLE,
                ImmutableSet.of(CassandraKeyValueServices.getMetadataCell(tableRef)), ts, deleteConsistency
        );

        try {
            cellValuePutter.put("put", AtlasDbConstants.DEFAULT_METADATA_TABLE,
                    KeyValueServices.toConstantTimestampValues(
                            ((Map<Cell, byte[]>) ImmutableMap.of(
                                    CassandraKeyValueServices.getMetadataCell(tableRef), meta)).entrySet(),
                            ts));
        } catch (Exception e) {
            throw QosAwareThrowables.unwrapAndThrowRateLimitExceededOrAtlasDbDependencyException(e);
        }

        new CellDeleter(clientPool, wrappingQueryRunner, deleteConsistency, System::currentTimeMillis).delete(
                AtlasDbConstants.DEFAULT_METADATA_TABLE, oldVersions);
    }
}
