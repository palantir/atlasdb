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

import static java.util.stream.Collectors.toSet;

import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;

import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.common.base.FunctionCheckedException;

public class LockTableTest {
    private LockTable lockTable;
    private CassandraClientPool clientPool;
    private CassandraKeyValueServiceConfig config;

    @Before
    public void setup() {
        config = CassandraTestSuite.CASSANDRA_KVS_CONFIG;
        clientPool = new CassandraClientPool(config);
        lockTable = LockTable.create(config, clientPool);
    }

    @Test
    public void shouldReturnConstantLockTableReference() throws Exception {
        assertThat(allPossibleLockTables(), contains(lockTable.getLockTable().getTablename()));
    }

    private Set<String> allPossibleLockTables() throws Exception {
        return clientPool.run((FunctionCheckedException<Cassandra.Client, Set<String>, Exception>)(client) -> {
            KsDef ksDef = client.describe_keyspace(config.keyspace());
            return ksDef.cf_defs.stream()
                    .map(CfDef::getName)
                    .filter(this::isLockTable)
                    .collect(toSet());
        });
    }

    private boolean isLockTable(String s) {
        return s.startsWith("_locks");
    }

}