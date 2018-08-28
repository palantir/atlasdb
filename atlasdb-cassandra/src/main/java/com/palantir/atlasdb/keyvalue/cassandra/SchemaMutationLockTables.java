/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Iterables;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.SafeArg;

public class SchemaMutationLockTables {
    public static final String LOCK_TABLE_PREFIX = "_locks";

    private static final Logger log = LoggerFactory.getLogger("SchemaMutationLockTables");
    private static final Predicate<String> IS_LOCK_TABLE = table -> table.startsWith(LOCK_TABLE_PREFIX);

    private final CassandraClientPool clientPool;
    private final CassandraKeyValueServiceConfig config;

    public SchemaMutationLockTables(CassandraClientPool clientPool, CassandraKeyValueServiceConfig config) {
        this.clientPool = clientPool;
        this.config = config;
    }

    public Set<TableReference> getAllLockTables() throws TException {
        Set<TableReference> lockTables = new HashSet<>();
        for (InetSocketAddress host : clientPool.getCurrentPools().keySet()) {
            lockTables.addAll(clientPool.runWithRetryOnHost(host, this::getAllLockTablesInternal));
        }
        return lockTables;
    }

    private Set<TableReference> getAllLockTablesInternal(CassandraClient client) throws TException {
        return client.describe_keyspace(config.getKeyspaceOrThrow()).getCf_defs().stream()
                .map(CfDef::getName)
                .filter(IS_LOCK_TABLE)
                .map(TableReference::createWithEmptyNamespace)
                .collect(Collectors.toSet());
    }

    public TableReference createLockTable() throws TException {
        TableReference lockTable = TableReference.createWithEmptyNamespace(LOCK_TABLE_PREFIX);

        try {
            log.info("Creating lock table {}", SafeArg.of("schemaMutationTableName", lockTable.getQualifiedName()));
            createTableWithCustomId(lockTable);
            return lockTable;
        } catch (InvalidRequestException ire) {
            // This can happen if multiple nodes concurrently attempt to create the locks table
            Set<TableReference> lockTables = getAllLockTables();
            if (lockTables.size() == 1) {
                log.info("Lock table was created by another node - returning it.");
                return Iterables.getOnlyElement(lockTables);
            } else {
                throw new IllegalStateException(String.format("Expected 1 locks table to exist, but found %d",
                        lockTables.size()));
            }
        }
    }

    private void createTableWithCustomId(TableReference tableRef) throws TException {
        String internalTableName = CassandraKeyValueServiceImpl.internalTableName(tableRef);
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

        clientPool.runWithRetry(client -> {
            try {
                client.execute_cql3_query(query, Compression.NONE, ConsistencyLevel.QUORUM);

                CassandraKeyValueServices.waitForSchemaVersions(
                        config,
                        client,
                        tableRef.getQualifiedName(),
                        true);
                return null;
            } catch (TException ex) {
                log.warn("Failed to create table", ex);
                throw ex;
            }
        });
    }
}
