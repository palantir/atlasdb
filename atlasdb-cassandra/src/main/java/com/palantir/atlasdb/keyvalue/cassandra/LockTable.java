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
import java.util.function.Predicate;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;

public class LockTable {
    public static final TableReference LOCK_TABLE = TableReference.createWithEmptyNamespace("_locks");
    private static Optional<String> currentLockTableName;
    private final CassandraKeyValueServiceConfig config;

    private final CassandraClientPool clientPool;

    private static final String ROW_NAME = "selected_lock_table";
    private static final String LOCK_TABLE_IS_CHOSEN_COLUMN = "lock_table_is_chosen";
    private static final Cell selectedLockTableCell = Cell.create(ROW_NAME.getBytes(), LOCK_TABLE_IS_CHOSEN_COLUMN.getBytes());


    public LockTable(CassandraKeyValueServiceConfig config, CassandraClientPool clientPool) {
        this.config = config;
        this.clientPool = clientPool;
        createUnderlyingTable();
    }

    public static LockTable create(CassandraKeyValueServiceConfig config, CassandraClientPool clientPool) {
        return new LockTable(config, clientPool);
    }

    private String createUnderlyingTable() {
        Optional<String> currentLockTableName = getCurrentLockTableName();
        if (currentLockTableName.isPresent()) {
            return currentLockTableName.get();
        }

        String ourLockTableName = createPossibleLockTable();
//        String winnerTableName = proposeTableToBeTheCorrectOne(ourLockTableName);
//        markAsWinner(winnerTableName);
//        removeLosers(winnerTableName);

        return ourLockTableName;
    }

    private String proposeTableToBeTheCorrectOne(String ourLockTableName) {
        // TODO implement
        return ourLockTableName;
    }

    private void markAsWinner(String winnerTableName) {

        // TODO get this from somewhere
        ConsistencyLevel writeConsistency = ConsistencyLevel.QUORUM;


        clientPool.run(client -> {
            try {
                return client.cas(
                        ByteBuffer.wrap(selectedLockTableCell.getRowName()),
                        winnerTableName,
                        ImmutableList.of(lockColumnWithValue(null)),
                        ImmutableList.of(lockColumnWithValue("yes".getBytes())),
                        ConsistencyLevel.SERIAL,
                        writeConsistency
                );
            } catch (TException e) {
                return null;
            }
        });
    }

    private Column lockColumnWithValue(byte[] value) {
        return new Column()
                .setName(CassandraKeyValueServices.makeCompositeBuffer(CassandraConstants.GLOBAL_DDL_LOCK_COLUMN_NAME.getBytes(), AtlasDbConstants.TRANSACTION_TS).array())
                .setValue(value) // expected previous
                .setTimestamp(AtlasDbConstants.TRANSACTION_TS);
    }


    private void removeLosers(String winnerTableName) {
        for (String tableName : getAllPossibleLockTables()) {
            if (!tableName.equals(winnerTableName)) {
                removeTable(tableName);
            }
        }
    }

    private void removeTable(String tableName) {
        // TODO implement
    }

    private String createPossibleLockTable() {
        String tentativeName = uniqueLocksTableName();
        try {
            clientPool.run(client -> {
                createTableInternal(client, TableReference.createWithEmptyNamespace(tentativeName));
                return null;
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
        return tentativeName;
    }

    private String uniqueLocksTableName() {
        // TODO implement
        return "_locks";
//        return "_locks_" + UUID.randomUUID();
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

    public Optional<String> getCurrentLockTableName() {
        return getAllPossibleLockTables().stream()
                .filter(hasBeenMarkedAsWinner())
                .findFirst();

    }

    private Predicate<String> hasBeenMarkedAsWinner() {
        return tableName -> {
            boolean winner = true;
            clientPool.run(client -> {
                try {
                    ColumnOrSuperColumn columnOrSuperColumn = client.get(
                            ByteBuffer.wrap(selectedLockTableCell.getRowName()),
                            new ColumnPath(LOCK_TABLE_IS_CHOSEN_COLUMN),
                            ConsistencyLevel.QUORUM);
                    byte[] value = columnOrSuperColumn.getColumn().getValue();
                    return new String(value).equals("yes");
                } catch (TException e) {
                    return null;
                }
            });
            return winner;
        };
    }

    public Set<String> getAllPossibleLockTables() {
        return null;
    }

    public TableReference getLockTable() {
        return LOCK_TABLE;
    }
}
