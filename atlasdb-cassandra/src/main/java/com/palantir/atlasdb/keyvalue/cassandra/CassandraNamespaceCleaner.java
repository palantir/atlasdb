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
import com.palantir.atlasdb.NamespaceCleaner;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.cassandra.CassandraServersConfigs.ThriftHostsExtractingVisitor;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraClientFactory.CassandraClientConfig;
import com.palantir.common.base.Throwables;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.refreshable.Refreshable;
import java.net.InetSocketAddress;
import org.apache.cassandra.thrift.Compression;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;

public final class CassandraNamespaceCleaner implements NamespaceCleaner {
    private final CassandraKeyValueServiceConfig config;

    private final Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig;
    private final String keyspace;

    public CassandraNamespaceCleaner(
            CassandraKeyValueServiceConfig config, Refreshable<CassandraKeyValueServiceRuntimeConfig> runtimeConfig) {
        this.config = config;
        this.runtimeConfig = runtimeConfig;
        keyspace = config.getKeyspaceOrThrow();
    }

    @Override
    public void dropAllTables() {
        try (CassandraClient client = getClient()) {
            CassandraKeyValueServices.runWithWaitingForSchemas(
                    () -> dropKeyspace(keyspace, client), config, client, "Dropping keyspace " + keyspace);
        } catch (Exception e) {
            throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
        }
    }

    @Override
    public boolean areAllTablesSuccessfullyDropped() {
        try (CassandraClient client = getClient()) {
            client.describe_keyspace(keyspace);
            return true;
        } catch (NotFoundException e) {
            return false;
        } catch (TException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private CassandraClient getClient() throws TException {
        return CassandraClientFactory.getClientInternal(getAnyCassandraProxy(), CassandraClientConfig.of(config));
    }

    private InetSocketAddress getAnyCassandraProxy() {
        return runtimeConfig.get().servers().accept(ThriftHostsExtractingVisitor.INSTANCE).stream()
                .findFirst()
                .orElseThrow(() -> new SafeIllegalStateException("No cassandra server found"));
    }

    private static void dropKeyspace(String keyspace, CassandraClient client) throws TException {
        String quotedKeyspace = wrapInQuotes(keyspace);

        CqlQuery query = CqlQuery.builder()
                .safeQueryFormat(
                        SchemaBuilder.dropKeyspace(quotedKeyspace).ifExists().buildInternal())
                .build();
        client.execute_cql3_query(query, Compression.NONE, CassandraKeyValueServiceImpl.WRITE_CONSISTENCY);
    }

    private static String wrapInQuotes(String string) {
        return "\"" + string + "\"";
    }
}
