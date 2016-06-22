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

import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.base.Throwables;

public class LockTable {
    public static final TableReference LOCK_TABLE = TableReference.createWithEmptyNamespace("_locks");

    public static LockTable create(CassandraKeyValueServiceConfigManager configManager, CassandraClientPool clientPool) {
        createUnderlyingTable(clientPool, configManager);
        return new LockTable();
    }

    private static void createUnderlyingTable(CassandraClientPool clientPool, CassandraKeyValueServiceConfigManager configManager) {
        try {
            clientPool.run(client -> {
                createTableInternal(client, LOCK_TABLE, configManager);
                return null;
            });
        } catch (Exception e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    // for tables internal / implementation specific to this KVS; these also don't get metadata in metadata table, nor do they show up in getTablenames, nor does this use concurrency control
    private static void createTableInternal(Cassandra.Client client, TableReference tableRef, CassandraKeyValueServiceConfigManager configManager) throws TException {
        CassandraKeyValueServiceConfig config = configManager.getConfig();
        if (tableAlreadyExists(client, internalTableName(tableRef), configManager)) {
            return;
        }
        CfDef cf = CassandraConstants.getStandardCfDef(config.keyspace(), internalTableName(tableRef));
        client.system_add_column_family(cf);
        CassandraKeyValueServices.waitForSchemaVersions(client, tableRef.getQualifiedName(), config.schemaMutationTimeoutMillis());
    }

    private static boolean tableAlreadyExists(Cassandra.Client client, String caseInsensitiveTableName, CassandraKeyValueServiceConfigManager configManager) throws TException {
        KsDef ks = client.describe_keyspace(configManager.getConfig().keyspace());
        for (CfDef cf : ks.getCf_defs()) {
            if (cf.getName().equalsIgnoreCase(caseInsensitiveTableName)) {
                return true;
            }
        }
        return false;
    }

    public TableReference getLockTable() {
        return LOCK_TABLE;
    }

    /**
     * This returns both active and inactive lock tables.
     */
    public Set<TableReference> getAllLockTables() {
        return ImmutableSet.of(LOCK_TABLE);
    }
}
