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
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class LockTableTest {

    private CassandraDataStore mockStore;
    private LockTableLeaderElector leaderElector;

    @Before
    public void setup() {
        mockStore = mock(CassandraDataStore.class);
        leaderElector = mock(LockTableLeaderElector.class);
    }

    @Test
    public void shouldCreateTheLockTableItSaysItHasCreated() throws Exception {
        TableReference tableRef = TableReference.createWithEmptyNamespace("_locks");

        LockTable.create(leaderElector, mockStore);

        verify(mockStore, times(1)).createTable(tableRef);
    }

    @Test
    public void shouldReturnNameDeterminedByLeaderElector() throws Exception {
        TableReference tableRef = TableReference.createWithEmptyNamespace("_locks_elected");
        when(mockStore.allTables()).thenReturn(ImmutableSet.of(tableRef));
        when(leaderElector.proposeTableToBeTheCorrectOne(any(TableReference.class))).thenReturn(tableRef);

        LockTable electedTable = LockTable.create(leaderElector, mockStore);
        assertThat(electedTable.getLockTable(), equalTo(tableRef));
    }

    @Test
    public void shouldMarkElectedTableAsWinner() throws Exception {
        LockTable lockTable = LockTable.create(leaderElector, mockStore);

        verify(mockStore, atLeastOnce()).put(lockTable.getLockTable(), "elected", "elected", "elected");
    }

    @Test
    public void shouldReturnPreElectedTable() throws Exception {
        TableReference tableRef = TableReference.createWithEmptyNamespace("_locks_elected");
        when(mockStore.allTables()).thenReturn(ImmutableSet.of(tableRef));
        when(mockStore.valueExists(tableRef, "elected", "elected", "elected")).thenReturn(true);

        LockTable lockTable = LockTable.create(leaderElector, mockStore);

        assertThat(lockTable.getLockTable(), equalTo(tableRef));
        verifyReturnedWithoutCreatingOrElectingNewTable(tableRef);
    }

    private void verifyReturnedWithoutCreatingOrElectingNewTable(TableReference tableRef) throws TException {
        verify(leaderElector, never()).proposeTableToBeTheCorrectOne(any(TableReference.class));
        verify(mockStore, never()).createTable(tableRef);
    }
}
