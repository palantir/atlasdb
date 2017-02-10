/**
 * Copyright 2017 Palantir Technologies
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.TimestampBoundStore;

public class CassandraTimestampBackupIntegrationTest {
    private static final long INITIAL_VALUE = CassandraTimestampUtils.INITIAL_VALUE;
    private static final long TIMESTAMP_1 = INITIAL_VALUE + 1000;
    private static final long TIMESTAMP_2 = TIMESTAMP_1 + 1000;
    private static final long TIMESTAMP_3 = TIMESTAMP_2 + 1000;

    private static final long TIMEOUT_SECONDS = 5L;
    private static final int POOL_SIZE = 32;

    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraTimestampIntegrationTest.class)
            .with(new CassandraContainer());

    private final CassandraKeyValueService kv = CassandraKeyValueService.create(
            CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.KVS_CONFIG),
            CassandraContainer.LEADER_CONFIG);
    private final ExecutorService executorService = Executors.newFixedThreadPool(POOL_SIZE);
    private final TimestampBoundStore timestampBoundStore = CassandraTimestampBoundStore.create(kv);
    private final CassandraTimestampBackupRunner backupRunner = new CassandraTimestampBackupRunner(kv);

    @Before
    public void setUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        backupRunner.createTimestampTable();
    }

    @After
    public void close() {
        kv.close();
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
        executeInParallelOnExecutorService(backupRunner::backupExistingTimestamp);
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
    public void throwsIfRestoringFromUnavailableBackup() {
        assertThatThrownBy(backupRunner::restoreFromBackup).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void throwsIfBothBoundsReadable() {
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
        executeInParallelOnExecutorService(backupRunner::restoreFromBackup);
        assertBoundEquals(INITIAL_VALUE);
    }

    @Test
    public void resilientToMultipleConcurrentBackupsAndRestores() {
        timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(TIMESTAMP_3);
        executeInParallelOnExecutorService(() -> {
            if (ThreadLocalRandom.current().nextBoolean()) {
                backupRunner.backupExistingTimestamp();
            } else {
                backupRunner.restoreFromBackup();
            }
        });
        backupRunner.restoreFromBackup();
        assertThat(timestampBoundStore.getUpperLimit()).isEqualTo(TIMESTAMP_3);
    }

    private void assertBoundEquals(long timestampBound) {
        assertThat(timestampBoundStore.getUpperLimit()).isEqualTo(timestampBound);
    }

    private void assertBoundNotReadable() {
        assertThatThrownBy(timestampBoundStore::getUpperLimit).isInstanceOf(IllegalArgumentException.class);
    }

    private void setupTwoReadableBoundsInKv() {
        backupRunner.backupExistingTimestamp();
        byte[] rowAndColumnNameBytes = PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME);
        kv.put(
                AtlasDbConstants.TIMESTAMP_TABLE,
                ImmutableMap.of(Cell.create(rowAndColumnNameBytes, rowAndColumnNameBytes), PtBytes.toBytes(0L)),
                Long.MAX_VALUE - 1);
    }

    private void executeInParallelOnExecutorService(Runnable runnable) {
        List<Future<?>> futures =
                Stream.generate(() -> executorService.submit(runnable))
                        .limit(POOL_SIZE)
                        .collect(Collectors.toList());
        futures.forEach(future -> {
            try {
                future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException exception) {
                throw Throwables.rewrapAndThrowUncheckedException(exception);
            }
        });
    }
}
