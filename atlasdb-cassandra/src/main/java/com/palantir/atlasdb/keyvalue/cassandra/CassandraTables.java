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
import java.util.stream.Stream;

import org.apache.cassandra.thrift.CfDef;
import org.apache.thrift.TException;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;

class CassandraTables {
    private final CassandraClientPool clientPool;
    private final CassandraKeyValueServiceConfig config;

    CassandraTables(CassandraClientPool clientPool, CassandraKeyValueServiceConfig config) {
        this.clientPool = clientPool;
        this.config = config;
    }

    Set<String> getExisting() {
        String keyspace = config.getKeyspaceOrThrow();

        try {
            return clientPool.runWithRetry(new FunctionCheckedException<CassandraClient, Set<String>, Exception>() {
                @Override
                public Set<String> apply(CassandraClient client) throws Exception {
                    return getExisting(client, keyspace);
                }

                @Override
                public String toString() {
                    return "describe_keyspace(" + keyspace + ")";
                }
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    private Set<String> getExisting(CassandraClient client, String keyspace) throws TException {
        return getTableNames(client, keyspace).collect(Collectors.toSet());
    }

    Stream<TableReference> getTableReferencesWithoutFiltering() {
        return getExisting().stream().map(TableReference::fromInternalTableName);
    }

    Set<String> getExistingLowerCased() throws TException {
        return getExistingLowerCased(config.getKeyspaceOrThrow());
    }

    private Set<String> getExistingLowerCased(String keyspace) throws TException {
        return clientPool.runWithRetry((client) -> getExistingLowerCased(client, keyspace));
    }

    private Set<String> getExistingLowerCased(CassandraClient client, String keyspace) throws TException {
        return getTableNames(client, keyspace).map(String::toLowerCase).collect(Collectors.toSet());
    }

    private Stream<String> getTableNames(CassandraClient client, String keyspace) throws TException {
        return client.describe_keyspace(keyspace).getCf_defs().stream().map(CfDef::getName);
    }
}
