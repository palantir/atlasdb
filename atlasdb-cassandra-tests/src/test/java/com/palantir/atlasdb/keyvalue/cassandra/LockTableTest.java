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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class LockTableTest {
    private LockTable lockTable;
    private CassandraClientPool clientPool;

    private CassandraDataStore cassandraDataStore;

    @Before
    public void setup() {
        CassandraKeyValueServiceConfig config = CassandraTestSuite.CASSANDRA_KVS_CONFIG;
        clientPool = new CassandraClientPool(config);
        lockTable = LockTable.create(config, clientPool);
        cassandraDataStore = new CassandraDataStore(config, clientPool);
    }

    @Test
    public void shouldCreateTheLockTableItSaysItHasCreated() throws Exception {
        assertThat(allPossibleLockTables(), hasItem(lockTable.getLockTable()));
    }

    @Test
    public void shouldReturnNameDeterminedByLeaderElector() throws Exception {
        CassandraDataStore mockStore = mock(CassandraDataStore.class);
        LockTableLeaderElector leaderElector = mock(LockTableLeaderElector.class);

        TableReference tableRef = TableReference.createWithEmptyNamespace("_locks_elected");
        when(mockStore.allTables()).thenReturn(ImmutableSet.of(tableRef));
        when(leaderElector.proposeTableToBeTheCorrectOne(any(TableReference.class))).thenReturn(tableRef);

        LockTable electedTable = LockTable.create(clientPool, leaderElector, mockStore);
        assertThat(electedTable.getLockTable(), equalTo(tableRef));
    }

    @Test
    public void shouldMarkElectedTableAsWinner() throws Exception {
        assertTrue(isMarkedAsWinner());
    }

    private boolean isMarkedAsWinner() throws Exception {
        return cassandraDataStore.valueExists(lockTable.getLockTable(), "elected", "elected", "elected");
    }

    private Set<TableReference> allPossibleLockTables() throws Exception {
        return cassandraDataStore.allTables().stream()
                .filter(this::isLockTable)
                .collect(Collectors.toSet());
    }

    private boolean isLockTable(TableReference tableReference) {
        return tableReference.getTablename().startsWith("_locks");
    }

}