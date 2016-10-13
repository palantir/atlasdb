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

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.KsDef;
import org.apache.thrift.TException;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.keyvalue.api.TableReference;

class CassandraDao {
    private final CassandraClientPool clientPool;
    private final CassandraKeyValueServiceConfigManager configManager;

    CassandraDao(CassandraClientPool clientPool, CassandraKeyValueServiceConfigManager configManager) {
        this.clientPool = clientPool;
        this.configManager = configManager;
    }

    Set<TableReference> getExistingTables() throws TException {
        return getExistingTables(configManager.getConfig().keyspace());
    }

    private Set<TableReference> getExistingTables(String keyspace) throws TException {
        return clientPool.runWithRetry((client) -> getExistingTablesLowerCased(client, keyspace));
    }

    private Set<TableReference> getExistingTablesLowerCased(Cassandra.Client client, String keyspace) throws TException {
        KsDef ks = client.describe_keyspace(keyspace);

        return ks.getCf_defs().stream()
                .map(cf -> CassandraKeyValueService.fromInternalTableName(cf.getName().toLowerCase()))
                .collect(Collectors.toSet()));
    }
}
