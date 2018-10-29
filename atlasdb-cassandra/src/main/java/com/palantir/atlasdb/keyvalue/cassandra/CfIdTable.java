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

import java.util.UUID;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.TException;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.cas.CheckAndSetResult;
import com.palantir.atlasdb.keyvalue.cassandra.cas.CheckAndSetRunner;
import com.palantir.logsafe.SafeArg;

public class CfIdTable {
    public static final String CF_ID_TABLE = "_cfid";
    public static final TableReference CF_ID_TABLEREF = TableReference.createWithEmptyNamespace(CF_ID_TABLE);

    private final CassandraClientPool clientPool;
    private final CassandraKeyValueServiceConfig config;
    private final CheckAndSetRunner checkAndSetRunner;

    public CfIdTable(CassandraClientPool clientPool,
            CassandraKeyValueServiceConfig config, CheckAndSetRunner checkAndSetRunner) {
        this.clientPool = clientPool;
        this.config = config;
        this.checkAndSetRunner = checkAndSetRunner;
    }

    public void createIfNotExists() {
        clientPool.runWithRetry(client -> {
            try {
                boolean alreadyExists = client.describe_keyspace(config.getKeyspaceOrThrow()).getCf_defs().stream()
                        .map(CfDef::getName)
                        .anyMatch(table -> table.equals(CF_ID_TABLE));
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
        UUID newId = UUID.randomUUID();
        Cell cell = Cell.create(PtBytes.toBytes(tableRef.toString()), PtBytes.toBytes("c"));
        return clientPool.runWithRetry(client -> {
            CheckAndSetRequest request = CheckAndSetRequest.newCell(CF_ID_TABLEREF, cell, PtBytes.toBytes(newId.toString()));
            try {
                CheckAndSetResult checkAndSetResult = checkAndSetRunner.executeCheckAndSet(client, request);
                if (checkAndSetResult.successful()) {
                    return newId;
                } else {
                    return UUID.fromString(PtBytes.toString(Iterables.getOnlyElement(checkAndSetResult.existingValues()).toByteArray()));
                }
            } catch (CheckAndSetException e) {
                return UUID.fromString(PtBytes.toString(Iterables.getOnlyElement(e.getActualValues())));
            } catch (TException e) {
                throw new RuntimeException();
            }
        });
    }

    private void createCfIdTable(CassandraClient client) throws TException {
        String internalTableName = CassandraKeyValueServiceImpl.internalTableName(CF_ID_TABLEREF);
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
                "creating the lock table " + CF_ID_TABLEREF.getQualifiedName());
    }
}
