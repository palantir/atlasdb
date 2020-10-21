/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import org.junit.Before;
import org.junit.Test;

public class CassandraTimestampStoreInvalidatorTest {
    private static final long BACKUP_TIMESTAMP = 42;

    private final CassandraTimestampBackupRunner backupRunner = mock(CassandraTimestampBackupRunner.class);
    private final CassandraTimestampStoreInvalidator invalidator = new CassandraTimestampStoreInvalidator(backupRunner);

    @Before
    public void before() {
        when(backupRunner.backupExistingTimestamp()).thenReturn(BACKUP_TIMESTAMP);
    }

    @Test
    public void throwsIfCreatingWithNonCassandraKvs() {
        assertThatThrownBy(() -> CassandraTimestampStoreInvalidator.create(new InMemoryKeyValueService(false)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void ensuresTablesExistOnInvalidate() {
        invalidator.backupAndInvalidate();
        verify(backupRunner, times(1)).ensureTimestampTableExists();
    }

    @Test
    public void backupReturnsTimestampFromBackupRunner() {
        assertThat(invalidator.backupAndInvalidate()).isEqualTo(BACKUP_TIMESTAMP);
        verify(backupRunner, times(1)).backupExistingTimestamp();
    }

    @Test
    public void ensuresTablesExistOnRevalidate() {
        invalidator.revalidateFromBackup();
        verify(backupRunner, times(1)).ensureTimestampTableExists();
    }

    @Test
    public void revalidateCallsRestoreOnBackupRunner() {
        invalidator.revalidateFromBackup();
        verify(backupRunner, times(1)).restoreFromBackup();
    }
}
