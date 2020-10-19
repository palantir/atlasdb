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
package com.palantir.atlasdb.factory.startup;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.common.exception.AtlasDbDependencyException;
import com.palantir.timestamp.TimestampManagementService;
import com.palantir.timestamp.TimestampStoreInvalidator;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TimeLockMigratorTest {
    private static final long BACKUP_TIMESTAMP = 42;
    private static final Exception EXCEPTION = new RuntimeException();

    @Mock private TimestampStoreInvalidator invalidator;
    @Mock private TimestampManagementService timestampManagementService;

    @Before
    public void before() {
        when(invalidator.backupAndInvalidate()).thenReturn(BACKUP_TIMESTAMP);
    }

    @After
    public void after() {
        verifyNoMoreInteractions(timestampManagementService);
        verifyNoMoreInteractions(invalidator);
    }

    @Test
    public void propagatesBackupTimestampToFastForwardOnRemoteService() {
        TimeLockMigrator migrator = TimeLockMigrator.create(timestampManagementService, invalidator);
        migrator.migrate();

        verify(timestampManagementService).ping();
        verify(invalidator, times(1)).backupAndInvalidate();
        verify(timestampManagementService).fastForwardTimestamp(BACKUP_TIMESTAMP);
    }

    @Test
    public void invalidationDoesNotProceedIfTimelockPingUnsuccessful() {
        when(timestampManagementService.ping()).thenThrow(EXCEPTION);

        TimeLockMigrator migrator = TimeLockMigrator.create(timestampManagementService, invalidator);
        assertThatThrownBy(migrator::migrate).isInstanceOf(AtlasDbDependencyException.class)
                .hasCause(EXCEPTION);

        verify(timestampManagementService).ping();
        verify(invalidator, never()).backupAndInvalidate();
    }

    @Test
    public void migrationDoesNotProceedIfInvalidationFails() {
        when(invalidator.backupAndInvalidate()).thenThrow(new IllegalStateException());

        TimeLockMigrator migrator = TimeLockMigrator.create(timestampManagementService, invalidator);
        assertThatThrownBy(migrator::migrate).isInstanceOf(IllegalStateException.class);

        verify(timestampManagementService).ping();
        verify(invalidator).backupAndInvalidate();
        verify(timestampManagementService, never()).fastForwardTimestamp(anyLong());
    }

    @Test
    public void asyncMigrationProceedsIfTimeLockInitiallyUnavailable() {
        when(timestampManagementService.ping())
                .thenThrow(EXCEPTION)
                .thenReturn(TimestampManagementService.PING_RESPONSE);

        TimeLockMigrator migrator = TimeLockMigrator.create(timestampManagementService, invalidator, true);
        migrator.migrate();

        waitUntilInitialized(migrator);

        verify(timestampManagementService, times(2)).ping();
        verify(invalidator, times(1)).backupAndInvalidate();
        verify(timestampManagementService).fastForwardTimestamp(BACKUP_TIMESTAMP);
    }

    @Test
    public void asyncMigrationProceedsIfInvalidatorInitiallyUnavailable() {
        when(invalidator.backupAndInvalidate())
                .thenThrow(new IllegalStateException("not ready yet"))
                .thenReturn(BACKUP_TIMESTAMP);

        TimeLockMigrator migrator = TimeLockMigrator.create(timestampManagementService, invalidator, true);
        migrator.migrate();

        waitUntilInitialized(migrator);

        verify(timestampManagementService, times(2)).ping();
        verify(invalidator, times(2)).backupAndInvalidate();
        verify(timestampManagementService).fastForwardTimestamp(BACKUP_TIMESTAMP);
    }

    private static void waitUntilInitialized(TimeLockMigrator migrator) {
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(migrator::isInitialized);
    }
}
