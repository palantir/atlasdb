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

import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueServices.StartTsResultsCollector;
import com.palantir.atlasdb.keyvalue.cassandra.cas.CheckAndSetQueries;
import com.palantir.atlasdb.keyvalue.cassandra.cas.CheckAndSetResult;
import com.palantir.atlasdb.keyvalue.cassandra.cas.CheckAndSetRunner;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.Throwables;
import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.logsafe.SafeArg;

class CfIdTable {
    private static final byte[] DELETION_MARKER = new byte[] {0};
    private static final long READ_TS = CheckAndSetQueries.CASSANDRA_TIMESTAMP + 1;

    private final CassandraClientPool clientPool;
    private final CassandraKeyValueServiceConfig config;
    private final CheckAndSetRunner checkAndSetRunner;
    private final MetricsManager metricsManager;
    private final CellLoader cellLoader;

    CfIdTable(CassandraClientPool clientPool, CassandraKeyValueServiceConfig config,
            CheckAndSetRunner checkAndSetRunner, MetricsManager metricsManager, CellLoader cellLoader) {
        this.clientPool = clientPool;
        this.config = config;
        this.checkAndSetRunner = checkAndSetRunner;
        this.metricsManager = metricsManager;
        this.cellLoader = cellLoader;
    }

    void create() {
        try {
            clientPool.runWithRetry(client -> {
                createCfIdTable(client);
                CassandraKeyValueServices.waitForSchemaVersions(config, client, "after creating the _cfId table");
                return null;
            });
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private void createCfIdTable(CassandraClient client) throws TException {
        String internalTableName = CassandraKeyValueServiceImpl.internalTableName(AtlasDbConstants.CF_ID_TABLE);
        String keyspace = config.getKeyspaceOrThrow();
        String fullTableNameForUuid = keyspace + "." + internalTableName;
        UUID uuid = UUID.nameUUIDFromBytes(fullTableNameForUuid.getBytes());

        String createTableStatement = "CREATE TABLE IF NOT EXISTS \"%s\".\"%s\" "
                + "(key blob, column1 blob, column2 bigint, value blob, PRIMARY KEY (key, column1, column2)) "
                + "WITH COMPACT STORAGE AND id = '%s'";
        CqlQuery query = CqlQuery.builder()
                .safeQueryFormat(createTableStatement)
                .addArgs(
                        SafeArg.of("keyspace", keyspace),
                        SafeArg.of("internalTableName", internalTableName),
                        SafeArg.of("cfId", uuid))
                .build();

        client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.QUORUM);

        CassandraKeyValueServices.waitForSchemaVersions(config, client,
                "creating the lock table " + AtlasDbConstants.CF_ID_TABLE.getQualifiedName());
    }

    UUID getCfIdForTable(TableReference tableRef) {
        return getExistingEntry(tableRef)
                .map(existing -> replaceIfDeleted(existing, tableRef))
                .orElseGet(() -> insertNew(tableRef));
    }

    void deleteCfIdForTable(TableReference tableRef) throws TException {
        Optional<byte[]> existing = getExistingEntry(tableRef);
        if (existing.isPresent()) {
            boolean success = deleteIfExists(existing.get(), tableRef);
            if (!success) {
                throw new AtlasDbDependencyException("Failed to mark cfId for table " + tableRef + " as deleted.");
            }
        }
    }

    private Optional<byte[]> getExistingEntry(TableReference tableRef) {
        StartTsResultsCollector collector = new StartTsResultsCollector(metricsManager, READ_TS);
        Cell cell = getCellForTable(tableRef);
        cellLoader.loadWithTs("get", AtlasDbConstants.CF_ID_TABLE, ImmutableSet.of(cell), READ_TS, false, collector,
                ConsistencyLevel.QUORUM);
        return Optional.ofNullable(collector.getCollectedResults().get(cell)).map(Value::getContents);
    }

    private static Cell getCellForTable(TableReference tableRef) {
        return Cell.create(PtBytes.toBytes(tableRef.toString()), PtBytes.toBytes("c"));
    }

    private UUID replaceIfDeleted(byte[] existing, TableReference tableRef) {
        if (markedAsDeleted(existing)) {
            return attemptCasToNewUuidAndReturnPersistedValue(tableRef, DELETION_MARKER);
        } else {
            return EncodingUtils.decodeUUID(existing, 0);
        }
    }

    private UUID insertNew(TableReference tableRef) {
        return attemptCasToNewUuidAndReturnPersistedValue(tableRef, null);
    }

    private boolean deleteIfExists(byte[] existing, TableReference tableRef) throws TException {
        if (markedAsDeleted(existing)) {
            return true;
        }

        CheckAndSetRequest request = new CheckAndSetRequest.Builder()
                .table(AtlasDbConstants.CF_ID_TABLE)
                .cell(getCellForTable(tableRef))
                .oldValueNullable(existing)
                .newValue(DELETION_MARKER)
                .build();
        return clientPool.runWithRetry(client -> {
            CheckAndSetResult checkAndSetResult = checkAndSetRunner.executeCheckAndSet(client, request);
            if (checkAndSetResult.successful()) {
                return true;
            } else {
                return markedAsDeleted(Iterables.getOnlyElement(checkAndSetResult.existingValues()).toByteArray());
            }
        });

    }

    private UUID attemptCasToNewUuidAndReturnPersistedValue(TableReference tableRef, byte[] oldValue) {
        UUID newId = UUID.randomUUID();
        CheckAndSetRequest request = new CheckAndSetRequest.Builder()
                .table(AtlasDbConstants.CF_ID_TABLE)
                .cell(getCellForTable(tableRef))
                .oldValueNullable(oldValue)
                .newValue(EncodingUtils.encodeUUID(newId))
                .build();
        try {
            return clientPool.runWithRetry(client -> {
                CheckAndSetResult checkAndSetResult = checkAndSetRunner.executeCheckAndSet(client, request);
                if (checkAndSetResult.successful()) {
                    return newId;
                } else {
                    return EncodingUtils.decodeUUID(
                            Iterables.getOnlyElement(checkAndSetResult.existingValues()).toByteArray(), 0);
                }
            });
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    private boolean markedAsDeleted(byte[] existing) {
        return Arrays.equals(existing, DELETION_MARKER);
    }
}
