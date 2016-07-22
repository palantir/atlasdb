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
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.UUID;

import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class UniqueSchemaMutationLockTableTest {
    public static final String LOCK_LEADER = "lockLeader";
    public static final String NOT_LOCK_LEADER = "notlockleader";
    private UniqueSchemaMutationLockTable uniqueLockTable;
    private SchemaMutationLockTables lockTables;
    private TableReference lockTable1 = TableReference.createWithEmptyNamespace(HiddenTables.LOCK_TABLE_PREFIX + UUID.randomUUID());
    private TableReference lockTable2 = TableReference.createWithEmptyNamespace(HiddenTables.LOCK_TABLE_PREFIX + UUID.randomUUID());

    private final ImmutableLeaderConfig isTheLockLeaderConfig = ImmutableLeaderConfig.builder()
            .addLeaders(LOCK_LEADER, NOT_LOCK_LEADER)
            .quorumSize(2)
            .localServer(LOCK_LEADER)
            .build();

    private final ImmutableLeaderConfig notTheLockLeaderConfig = isTheLockLeaderConfig.withLocalServer(NOT_LOCK_LEADER);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setupKVS() throws TException, InterruptedException {
        lockTables = mock(SchemaMutationLockTables.class);
        uniqueLockTable = new UniqueSchemaMutationLockTable(lockTables, isTheLockLeaderConfig);
    }


    @Test
    public void shouldReturnALockTableIfNoneExist() throws TException {
        when(lockTables.getAllLockTables()).thenReturn(Collections.EMPTY_SET);
        when(lockTables.createLockTable(any(UUID.class))).thenReturn(lockTable1);

        assertThat(uniqueLockTable.getOnlyTable(), is(lockTable1));
    }

    @Test
    public void shouldReturnTheSameLockTableOnMultipleCalls() throws TException {
        when(lockTables.getAllLockTables()).thenReturn(ImmutableSet.of(lockTable1));

        assertThat(uniqueLockTable.getOnlyTable(), is(uniqueLockTable.getOnlyTable()));
    }

    @Test
    public void shouldNotCreateALockTableIfOneAlreadyExists() throws Exception {
        when(lockTables.getAllLockTables()).thenReturn(ImmutableSet.of(lockTable1));

        uniqueLockTable.getOnlyTable();

        verify(lockTables, never()).createLockTable(any(UUID.class));
    }

    @Test
    public void shouldReturnTheLockTableIfItIsTheOnlyOne() throws TException {
        when(lockTables.getAllLockTables()).thenReturn(ImmutableSet.of(lockTable1));

        assertThat(uniqueLockTable.getOnlyTable(), is(lockTable1));
    }

    @Test
    public void shouldThrowExceptionIfMultipleTablesExist() throws Exception {
        when(lockTables.getAllLockTables()).thenReturn(ImmutableSet.of(lockTable1, lockTable2));

        exception.expect(IllegalArgumentException.class);
        try {
            uniqueLockTable.getOnlyTable();
        } finally {
            verify(lockTables, never()).createLockTable(any(UUID.class));
        }
    }

    @Test
    public void shouldThrowExceptionIfMultipleTablesExistSuddenlyDueToConcurrency() throws Exception {
        when(lockTables.getAllLockTables()).thenReturn(ImmutableSet.of(), ImmutableSet.of(lockTable1, lockTable2));

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Multiple schema mutation lock tables have been created.\n");

        try {
            uniqueLockTable.getOnlyTable();
        } finally {
            verify(lockTables).createLockTable(any(UUID.class));
        }
    }

    @Test
    public void shouldNotCreateLockTableIfNotLockLeader() throws Exception {
        when(lockTables.getAllLockTables()).thenReturn(ImmutableSet.of(), ImmutableSet.of(lockTable1));

        uniqueLockTable = new UniqueSchemaMutationLockTable(lockTables, notTheLockLeaderConfig);

        uniqueLockTable.getOnlyTable();

        verify(lockTables, never()).createLockTable(any(UUID.class));

    }
}

