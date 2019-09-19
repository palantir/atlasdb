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

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.flake.ShouldRetry;
import com.palantir.timestamp.TimestampBoundStore;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

@ShouldRetry
public class CassandraTimestampBackupIntegrationTest {
    private static final long INITIAL_VALUE = CassandraTimestampUtils.INITIAL_VALUE;
    private static final long TIMESTAMP_1 = INITIAL_VALUE + 1000;
    private static final long TIMESTAMP_2 = TIMESTAMP_1 + 1000;
    private static final long TIMESTAMP_3 = TIMESTAMP_2 + 1000;

    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    private final CassandraKeyValueService kv = CASSANDRA.getDefaultKvs();
    private final TimestampBoundStore timestampBoundStore = CassandraTimestampBoundStore.create(kv);
    private final CassandraTimestampBackupRunner backupRunner = new CassandraTimestampBackupRunner(kv);

    @Before
    public void setUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        backupRunner.ensureTimestampTableExists();
    }

    @Test
    public void canBackupWithDefaultValue() {
        assertThat(backupRunner.backupExistingTimestamp())
                .isEqualTo(CassandraTimestampUtils.INITIAL_VALUE);
    }

    @Test
    public void canBackupAlreadyStoredBound() {
        timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(TIMESTAMP_1);
        assertThat(backupRunner.backupExistingTimestamp()).isEqualTo(TIMESTAMP_1);
    }

    @Test
    public void cannotReadAfterBackup() {
        backupRunner.backupExistingTimestamp();
        assertBoundNotReadable();
    }

    @Test
    public void canBackupMultipleTimes() {
        assertThat(backupRunner.backupExistingTimestamp()).isEqualTo(INITIAL_VALUE);
        assertThat(backupRunner.backupExistingTimestamp()).isEqualTo(INITIAL_VALUE);
        assertThat(backupRunner.backupExistingTimestamp()).isEqualTo(INITIAL_VALUE);
        assertBoundNotReadable();
    }

    @Test
    public void resilientToMultipleConcurrentBackups() {
        CassandraTestTools.executeInParallelOnExecutorService(backupRunner::backupExistingTimestamp);
        assertBoundNotReadable();
        backupRunner.restoreFromBackup();
        assertBoundEquals(INITIAL_VALUE);
    }

    @Test
    public void backsUpDefaultValueIfNoBoundExists() {
        backupRunner.backupExistingTimestamp();
        backupRunner.restoreFromBackup();
        assertBoundEquals(INITIAL_VALUE);
    }

    @Test
    public void backsUpKnownBoundIfExists() {
        timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(TIMESTAMP_1);
        backupRunner.backupExistingTimestamp();
        backupRunner.restoreFromBackup();
        assertBoundEquals(TIMESTAMP_1);
    }

    @Test
    public void restoreDoesNothingIfTimestampIsReadable() {
        backupRunner.restoreFromBackup();
        assertBoundEquals(INITIAL_VALUE);
    }

    @Test
    public void backupThrowsIfBothBoundsReadable() {
        setupTwoReadableBoundsInKv();
        assertThatThrownBy(backupRunner::backupExistingTimestamp).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void restoreThrowsIfBothBoundsReadable() {
        setupTwoReadableBoundsInKv();
        assertThatThrownBy(backupRunner::restoreFromBackup).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void ignoresInvalidBackupIfDataAvailable() {
        timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(TIMESTAMP_3);
        backupRunner.restoreFromBackup();
        assertBoundEquals(TIMESTAMP_3);
    }

    @Test
    public void canBackupAndRestoreMultipleTimes() {
        timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(TIMESTAMP_1);
        backupRunner.backupExistingTimestamp();
        assertBoundNotReadable();
        backupRunner.restoreFromBackup();
        assertBoundEquals(TIMESTAMP_1);

        timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(TIMESTAMP_2);
        backupRunner.backupExistingTimestamp();
        assertBoundNotReadable();
        backupRunner.restoreFromBackup();
        assertBoundEquals(TIMESTAMP_2);
    }

    @Test
    public void canRestoreTwice() {
        backupRunner.backupExistingTimestamp();
        backupRunner.restoreFromBackup();
        backupRunner.restoreFromBackup();
        assertBoundEquals(INITIAL_VALUE);
    }

    @Test
    public void multipleRestoresDoNotMakeUsGoBackInTime() {
        backupRunner.backupExistingTimestamp();
        backupRunner.restoreFromBackup();
        timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(TIMESTAMP_2);
        backupRunner.restoreFromBackup();
        assertBoundEquals(TIMESTAMP_2); // in particular, not TIMESTAMP_1
    }

    @Test
    public void resilientToMultipleConcurrentRestores() {
        backupRunner.backupExistingTimestamp();
        CassandraTestTools.executeInParallelOnExecutorService(() -> {
            CassandraTimestampBackupRunner runner = new CassandraTimestampBackupRunner(kv);
            runner.restoreFromBackup();
        });
        assertBoundEquals(INITIAL_VALUE);
    }

    @Test
    public void stateIsWellDefinedEvenUnderConcurrentBackupsAndRestores() {
        timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(TIMESTAMP_3);
        CassandraTestTools.executeInParallelOnExecutorService(() -> {
            CassandraTimestampBackupRunner runner = new CassandraTimestampBackupRunner(kv);
            try {
                if (ThreadLocalRandom.current().nextBoolean()) {
                    runner.backupExistingTimestamp();
                } else {
                    runner.restoreFromBackup();
                }
            } catch (IllegalStateException exception) {
                // This is possible under concurrent backup *and* restore.
                // The point of this test is to ensure that the table is in a well defined state at the end.
            }
        });
        backupRunner.restoreFromBackup();
        assertThat(timestampBoundStore.getUpperLimit()).isEqualTo(TIMESTAMP_3);
    }

    @Test
    public void backupThrowsIfTimestampTableDoesNotExist() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThatThrownBy(backupRunner::backupExistingTimestamp).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void restoreThrowsIfTimestampTableDoesNotExist() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        assertThatThrownBy(backupRunner::restoreFromBackup).isInstanceOf(IllegalStateException.class);
    }

    private void assertBoundEquals(long timestampBound) {
        assertThat(timestampBoundStore.getUpperLimit()).isEqualTo(timestampBound);
    }

    private void assertBoundNotReadable() {
        assertThatThrownBy(timestampBoundStore::getUpperLimit).isInstanceOf(IllegalStateException.class);
    }

    private void setupTwoReadableBoundsInKv() {
        backupRunner.backupExistingTimestamp();
        byte[] rowAndColumnNameBytes = PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME);
        kv.put(AtlasDbConstants.TIMESTAMP_TABLE,
                ImmutableMap.of(Cell.create(rowAndColumnNameBytes, rowAndColumnNameBytes), PtBytes.toBytes(0L)),
                Long.MAX_VALUE - 1);
    }
}
