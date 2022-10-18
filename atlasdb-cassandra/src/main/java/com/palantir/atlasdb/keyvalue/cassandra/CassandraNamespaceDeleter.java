/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.namespacedeleter.NamespaceDeleter;
import com.palantir.common.base.Throwables;
import java.io.IOException;
import java.util.function.Supplier;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;

public final class CassandraNamespaceDeleter implements NamespaceDeleter {
    private final CassandraKeyValueServiceConfig config;
    private final Supplier<CassandraClient> cassandraClientSupplier;
    private final Namespace keyspace;

    public CassandraNamespaceDeleter(
            CassandraKeyValueServiceConfig config, Supplier<CassandraClient> cassandraClientSupplier) {
        this.config = config;
        this.cassandraClientSupplier = cassandraClientSupplier;
        // Namespace performs some handy validation that also ensures we don't allow sql injection characters in the
        // keyspace!
        keyspace = Namespace.create(config.getKeyspaceOrThrow());
    }

    @Override
    public void deleteAllDataFromNamespace() {
        try (CassandraClient client = cassandraClientSupplier.get()) {
            CassandraKeyValueServices.runWithWaitingForSchemas(
                    () -> dropKeyspace(keyspace, client), config, client, "Dropping keyspace " + keyspace);
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    @Override
    public boolean isNamespaceDeletedSuccessfully() {
        try (CassandraClient client = cassandraClientSupplier.get()) {
            client.describe_keyspace(keyspace.getName());
            return false;
        } catch (NotFoundException e) {
            return true;
        } catch (TException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private static void dropKeyspace(Namespace namespace, CassandraClient client) throws TException {
        CqlQuery query = CqlQuery.builder()
                .safeQueryFormat(SchemaBuilder.dropKeyspace(wrapInQuotes(namespace))
                        .ifExists()
                        .buildInternal())
                .build();

        client.execute_cql3_query(query, Compression.NONE, CassandraKeyValueServiceImpl.WRITE_CONSISTENCY);
    }

    private static String wrapInQuotes(Namespace namespace) {
        return "\"" + namespace.getName() + "\"";
    }

    @Override
    public void close() throws IOException {
        // no-op
    }
}
