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

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.base.Throwables;
import java.util.Collection;
import org.apache.thrift.TException;

class CassandraTableTruncator {
    private final TracingQueryRunner queryRunner;
    private final CassandraClientPool clientPool;

    CassandraTableTruncator(TracingQueryRunner queryRunner, CassandraClientPool clientPool) {
        this.queryRunner = queryRunner;
        this.clientPool = clientPool;
    }

    void truncateTables(Collection<TableReference> tablesToTruncate) {
        if (!tablesToTruncate.isEmpty()) {
            try {
                runTruncateInternal(tablesToTruncate);
            } catch (TException e) {
                throw Throwables.unwrapAndThrowAtlasDbDependencyException(e);
            }
        }
    }

    private void runTruncateInternal(Collection<TableReference> tablesToTruncate) throws TException {
        clientPool.runWithRetry(client -> runTruncateOnClient(tablesToTruncate, client));
    }

    Void runTruncateOnClient(Collection<TableReference> tablesToTruncate, CassandraClient client) throws TException {
        truncationFunction(tablesToTruncate).apply(client);
        return null;
    }

    private FunctionCheckedException<CassandraClient, Void, TException> truncationFunction(
            Collection<TableReference> tablesToTruncate) {
        return new FunctionCheckedException<CassandraClient, Void, TException>() {
            @Override
            public Void apply(CassandraClient client) throws TException {
                for (TableReference tableRef : tablesToTruncate) {
                    queryRunner.run(client, tableRef, () -> {
                        client.truncate(CassandraKeyValueServiceImpl.internalTableName(tableRef));
                        return true;
                    });
                }
                return null;
            }

            @Override
            public String toString() {
                return "truncateTables(" + tablesToTruncate + ")";
            }
        };
    }
}
