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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

import java.util.UUID;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;

public class SchemaMutationLockTablesTest {
    private SchemaMutationLockTables lockTables;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setupKVS() throws TException, InterruptedException {
        CassandraKeyValueServiceConfig config = CassandraTestSuite.CASSANDRA_KVS_CONFIG
                .withKeyspace(UUID.randomUUID().toString().replace('-', '_'));
        CassandraClientPool clientPool = new CassandraClientPool(config);
        clientPool.runOneTimeStartupChecks();
        lockTables = new SchemaMutationLockTables(clientPool, config);
    }

    @Test
    public void startsWithNoTables() throws TException {
        assertThat(lockTables.getAllLockTables(), is(empty()));
    }
}
