/*
 * Copyright 2016 Palantir Technologies
 * ​
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * ​
 * http://opensource.org/licenses/BSD-3-Clause
 * ​
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.cassandra;

import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class CassandraDataStoreTest {
    @Test
    public void shouldCreateATable() throws Exception {
        CassandraKeyValueServiceConfig config = CassandraTestSuite.CASSANDRA_KVS_CONFIG;
        CassandraClientPool clientPool = new CassandraClientPool(config);

        CassandraDataStore cassandraDataStore = new CassandraDataStore(config, clientPool);
        String tableName = "cassandra_data_store_test_table";

        cassandraDataStore.createTable(tableName);

        assertThat(cassandraDataStore.allTables(), hasItem(TableReference.fromString(tableName)));
    }
}
