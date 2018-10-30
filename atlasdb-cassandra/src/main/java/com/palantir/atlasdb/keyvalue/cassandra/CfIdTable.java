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

import org.apache.cassandra.thrift.CfDef;
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
import com.palantir.atlasdb.keyvalue.cassandra.cas.CheckAndSetResult;
import com.palantir.atlasdb.keyvalue.cassandra.cas.CheckAndSetRunner;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.SafeArg;

public class CfIdTable {
    private static final byte[] DELETION_MARKER = new byte[] {0};

    private final CassandraClientPool clientPool;
    private final CassandraKeyValueServiceConfig config;
    private final CheckAndSetRunner checkAndSetRunner;
    private final MetricsManager metricsManager;
    private final CellLoader cellLoader;



    public CfIdTable(CassandraClientPool clientPool,
            CassandraKeyValueServiceConfig config, CheckAndSetRunner checkAndSetRunner,
            MetricsManager metricsManager, CellLoader cellLoader) {
        this.clientPool = clientPool;
        this.config = config;
        this.checkAndSetRunner = checkAndSetRunner;
        this.metricsManager = metricsManager;
        this.cellLoader = cellLoader;
    }

    private Optional<byte[]> getExistingEntry(TableReference tableRef) {
        CassandraKeyValueServices.StartTsResultsCollector collector = new CassandraKeyValueServices.StartTsResultsCollector(metricsManager, 1);
        Cell cell = getCellForTable(tableRef);
        cellLoader.loadWithTs("get", AtlasDbConstants.CF_ID_TABLE, ImmutableSet.of(cell), 1, false, collector, ConsistencyLevel.QUORUM);
        return Optional.ofNullable(collector.getCollectedResults().get(cell)).map(Value::getContents);
    }

    public void createIfNotExists() {
        clientPool.runWithRetry(client -> {
            try {
                boolean alreadyExists = client.describe_keyspace(config.getKeyspaceOrThrow()).getCf_defs().stream()
                        .map(CfDef::getName)
                        .anyMatch(table -> table.equals(AtlasDbConstants.CF_ID_TABLE.getTablename()));
                if (!alreadyExists) {
                    createCfIdTable(client);
                }
            } catch (TException e) {
                throw new RuntimeException();
            }
            return null;
        });
    }

    public UUID getCfIdForTable(TableReference tableRef) {
        return getExistingEntry(tableRef)
                .map(existing -> replaceIfDeleted(existing, tableRef))
                .orElseGet(() -> insertNew(tableRef));
    }

    private UUID replaceIfDeleted(byte[] existing, TableReference tableRef) {
        if (deletionMarker(existing)) {
            return checkAndSetAndReturnCurrent(tableRef, DELETION_MARKER);
        } else {
            return EncodingUtils.decodeUUID(existing, 0);
        }
    }

    private UUID insertNew(TableReference tableRef) {
        return checkAndSetAndReturnCurrent(tableRef, null);
    }

    public boolean deleteCfIdForTable(TableReference tableRef) {
        return getExistingEntry(tableRef).map(existing -> deleteIfNecessary(existing, tableRef)).orElse(true);
    }

    private boolean deleteIfNecessary(byte[] existing, TableReference tableRef) {
        if (!deletionMarker(existing)) {
            CheckAndSetRequest request = new CheckAndSetRequest.Builder()
                    .table(AtlasDbConstants.CF_ID_TABLE)
                    .cell(getCellForTable(tableRef))
                    .oldValueNullable(existing)
                    .newValue(DELETION_MARKER)
                    .build();
            return clientPool.runWithRetry(client -> {
                try {
                    CheckAndSetResult checkAndSetResult = checkAndSetRunner.executeCheckAndSet(client, request);
                    if (checkAndSetResult.successful()) {
                        return true;
                    } else {
                        return deletionMarker(Iterables.getOnlyElement(checkAndSetResult.existingValues()).toByteArray());
                    }
                } catch (TException e) {
                    // todo(gmaretic): make unsketchy
                    throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
                }
            });
        }
        return true;
    }


    private UUID checkAndSetAndReturnCurrent(TableReference tableRef, byte[] oldValue) {
        UUID newId = UUID.randomUUID();
        CheckAndSetRequest request = new CheckAndSetRequest.Builder()
                .table(AtlasDbConstants.CF_ID_TABLE)
                .cell(getCellForTable(tableRef))
                .oldValueNullable(oldValue)
                .newValue(EncodingUtils.encodeUUID(newId))
                .build();
        return clientPool.runWithRetry(client -> {
            try {
                CheckAndSetResult checkAndSetResult = checkAndSetRunner.executeCheckAndSet(client, request);
                if (checkAndSetResult.successful()) {
                    return newId;
                } else {
                    return EncodingUtils.decodeUUID(
                            Iterables.getOnlyElement(checkAndSetResult.existingValues()).toByteArray(), 0);
                }
            } catch (TException e) {
                // todo(gmaretic): make unsketchy
                throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
            }
        });
    }



    private boolean deletionMarker(byte[] existing) {
        return Arrays.equals(existing, DELETION_MARKER);
    }

    private static Cell getCellForTable(TableReference tableRef) {
        return Cell.create(PtBytes.toBytes(tableRef.toString()), PtBytes.toBytes("c"));
    }

    private void createCfIdTable(CassandraClient client) throws TException {
        String internalTableName = CassandraKeyValueServiceImpl.internalTableName(AtlasDbConstants.CF_ID_TABLE);
        String keyspace = config.getKeyspaceOrThrow();
        String fullTableNameForUuid = keyspace + "." + internalTableName;
        UUID uuid = UUID.nameUUIDFromBytes(fullTableNameForUuid.getBytes());

        String createTableStatement = "CREATE TABLE \"%s\".\"%s\" (\n"
                + "    key blob,\n"
                + "    column1 blob,\n"
                + "    column2 bigint,\n"
                + "    value blob,\n"
                + "    PRIMARY KEY (key, column1, column2)\n"
                + ") WITH COMPACT STORAGE\n"
                + "    AND id = '%s'";
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
}
