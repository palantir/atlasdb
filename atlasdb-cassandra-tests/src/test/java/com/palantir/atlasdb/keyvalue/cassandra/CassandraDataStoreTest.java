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
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class CassandraDataStoreTest {

    private CassandraDataStore cassandraDataStore;

    @Before
    public void setUp() {
        CassandraKeyValueServiceConfig config = CassandraTestSuite.CASSANDRA_KVS_CONFIG;
        CassandraClientPool clientPool = new CassandraClientPool(config);

        cassandraDataStore = new CassandraDataStore(config, clientPool);
    }

    @Test
    public void shouldCreateATable() throws Exception {
        TableReference tableReference = TableReference.createWithEmptyNamespace("cassandra_data_store_test_table");

        cassandraDataStore.createTable(tableReference);

        assertThat(cassandraDataStore.allTables(), hasItem(tableReference));
    }

    @Test
    public void shouldPut() throws Exception {
        String rowName = "key";
        String columnName = "col";
        String value = "val";
        TableReference tableReference = TableReference.createWithEmptyNamespace("cassandra_data_store_test_put");

        cassandraDataStore.createTable(tableReference);
        cassandraDataStore.put(tableReference, rowName, columnName, value);

        assertTrue(cassandraDataStore.valueExists(tableReference, rowName, columnName, value));
    }
}
