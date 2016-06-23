/**
 * Copyright 2016 Palantir Technologies
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

import static com.palantir.atlasdb.keyvalue.cassandra.CassandraClientPool.internalTableName;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.Set;

import org.apache.cassandra.thrift.CASResult;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;

public class LockTable {
    public final TableReference lockTable;

    public LockTable(String ref) {
        this.lockTable = TableReference.createWithEmptyNamespace(ref);
    }

    public static LockTable create(CassandraKeyValueServiceConfig config, CassandraClientPool clientPool) {
        return create(config, clientPool, new LockTableLeaderElector());
    }

    public static LockTable create(CassandraKeyValueServiceConfig config, CassandraClientPool clientPool, LockTableLeaderElector leaderElector) {
        String ref = new LockTableCreator(config, clientPool, leaderElector).create();
        return new LockTable(ref);
    }

    private static class LockTableCreator {
        private final CassandraKeyValueServiceConfig config;
        private final CassandraClientPool clientPool;
        private final LockTableLeaderElector leaderElector;

        public LockTableCreator(CassandraKeyValueServiceConfig config, CassandraClientPool clientPool, LockTableLeaderElector leaderElector) {
            this.config = config;
            this.clientPool = clientPool;
            this.leaderElector = leaderElector;
        }

        public String create() {
            // Check if ANY lock table exists already
            // if so, return the name
            Optional<String> currentLockTableName = getCurrentLockTableName();
            if (currentLockTableName.isPresent()) {
                return currentLockTableName.get();
            }

            String ourLockTableName = createPossibleLockTable();

            String winnerTableName = leaderElector.proposeTableToBeTheCorrectOne(ourLockTableName);
            markAsWinner(winnerTableName);
    /*
            removeLosers(winnerTableName);
     */
            return winnerTableName;
        }

        private Optional<String> getCurrentLockTableName() {
            return Optional.empty();
        }

        private String createPossibleLockTable() {
            try {
                String tableName = "_locks";
                createTable(tableName);
                return tableName;
            } catch (Exception e) {
                throw Throwables.throwUncheckedException(e);
            }
        }

        protected void createTable(String tableName) throws TException {
            clientPool.run(client -> {
                createTableInternal(client, TableReference.createWithEmptyNamespace(tableName));
                return null;
            });
        }

        // for tables internal / implementation specific to this KVS; these also don't get metadata in metadata table, nor do they show up in getTablenames, nor does this use concurrency control
        private void createTableInternal(Cassandra.Client client, TableReference tableRef) throws TException {
            if (tableAlreadyExists(client, internalTableName(tableRef))) {
                return;
            }
            CfDef cf = CassandraConstants.getStandardCfDef(config.keyspace(), internalTableName(tableRef));
            client.system_add_column_family(cf);
            CassandraKeyValueServices.waitForSchemaVersions(client, tableRef.getQualifiedName(), config.schemaMutationTimeoutMillis());
        }

        private boolean tableAlreadyExists(Cassandra.Client client, String caseInsensitiveTableName) throws TException {
            KsDef ks = client.describe_keyspace(config.keyspace());
            for (CfDef cf : ks.getCf_defs()) {
                if (cf.getName().equalsIgnoreCase(caseInsensitiveTableName)) {
                    return true;
                }
            }
            return false;
        }

        // TODO - this fails for some reason, "java.lang.RuntimeException: InvalidRequestException(why:Not enough bytes to read value of component 0)"
        private void markAsWinner(String winnerTableName) {
            clientPool.run(client -> {
                try {
                    byte[] elected = "elected".getBytes();
                    byte[] colName = CassandraKeyValueServices.makeCompositeBuffer(elected, 0L).array();
                    Column column = new Column()
                            .setName(colName)
                            .setValue(elected)
                            .setTimestamp(0L);
                    ByteBuffer rowName = ByteBuffer.wrap(elected);
                    CASResult result = client.cas(
                            rowName,
                            winnerTableName,
                            ImmutableList.of(),
                            ImmutableList.of(column),
                            ConsistencyLevel.SERIAL,
                            ConsistencyLevel.QUORUM
                    );
                } catch (TException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        }
    }

    public TableReference getLockTable() {
        return lockTable;
    }

    /**
     * This returns both active and inactive lock tables.
     */
    public Set<TableReference> getAllLockTables() {
        return ImmutableSet.of(lockTable);
    }
}
