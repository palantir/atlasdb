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
import java.util.Optional;
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

public class CassandraTimestampStoreIntegrationTest {
    private static final long TIMESTAMP_1 = 1L;
    private static final long TIMESTAMP_2 = 2L;
    private static final long TIMESTAMP_3 = 3L;

    private static final long TIMEOUT_SECONDS = 5L;
    private static final int POOL_SIZE = 16;

    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraTimestampIntegrationTest.class)
            .with(new CassandraContainer());

    private final CassandraKeyValueService kv = CassandraKeyValueService.create(
            CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.KVS_CONFIG),
            CassandraContainer.LEADER_CONFIG);
    private final CassandraTimestampStore store = new CassandraTimestampStore(kv);
    private final ExecutorService executorService = Executors.newFixedThreadPool(POOL_SIZE);

    @Before
    public void setUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        store.createTimestampTable();
    }

    @After
    public void close() {
        kv.close();
    }

    @Test
    public void canReadEmptyBoundWhenNothingIsStored() {
        assertThat(store.getUpperLimit()).isEqualTo(Optional.empty());
    }

    @Test
    public void canReadStoredBounds() {
        store.storeTimestampBound(Optional.empty(), TIMESTAMP_1);
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_1));
    }

    @Test
    public void readsTheLatestStoredBoundGoingForwards() {
        store.storeTimestampBound(Optional.empty(), TIMESTAMP_1);
        store.storeTimestampBound(Optional.of(TIMESTAMP_1), TIMESTAMP_2);
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_2));
    }

    @Test
    public void readsTheLatestStoredBoundGoingBackwards() {
        store.storeTimestampBound(Optional.empty(), TIMESTAMP_2);
        store.storeTimestampBound(Optional.of(TIMESTAMP_2), TIMESTAMP_1);
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_1));
    }

    @Test
    public void doesNotSucceedIfStoringBoundWithExpectationWhenThereIsNone() {
        assertThat(store.storeTimestampBound(Optional.of(TIMESTAMP_1), TIMESTAMP_2).successful()).isFalse();
        assertThat(store.getUpperLimit()).isEqualTo(Optional.empty());
    }

    @Test
    public void doesNotSucceedIfStoringBoundWithIncorrectExpectation() {
        store.storeTimestampBound(Optional.empty(), TIMESTAMP_1);

        assertThat(store.storeTimestampBound(Optional.of(TIMESTAMP_2), TIMESTAMP_3).successful()).isFalse();
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_1));
    }

    @Test
    public void throwsIfReadingInvalidatedTimestampBound() {
        store.backupExistingTimestamp(TIMESTAMP_1);
        assertThatThrownBy(store::getUpperLimit).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void throwsIfStoringToABackedUpStore() {
        store.storeTimestampBound(Optional.empty(), TIMESTAMP_1);
        assertThat(store.backupExistingTimestamp(TIMESTAMP_3)).isEqualTo(TIMESTAMP_1);
        assertThatThrownBy(() -> store.storeTimestampBound(Optional.of(TIMESTAMP_1), TIMESTAMP_2))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void throwsOnBackupOrRestoreWithTwoReadableBounds() {
        store.backupExistingTimestamp(TIMESTAMP_1);
        byte[] rowAndColumnNameBytes = PtBytes.toBytes(CassandraTimestampUtils.ROW_AND_COLUMN_NAME);
        kv.put(AtlasDbConstants.TIMESTAMP_TABLE,
                ImmutableMap.of(Cell.create(rowAndColumnNameBytes, rowAndColumnNameBytes),
                        PtBytes.toBytes(TIMESTAMP_2)),
                Long.MAX_VALUE - 1);
        assertThatThrownBy(() -> store.backupExistingTimestamp(TIMESTAMP_2))
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(store::restoreFromBackup).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void backsUpStoredBound() {
        store.storeTimestampBound(Optional.empty(), TIMESTAMP_1);
        store.backupExistingTimestamp(TIMESTAMP_2);
        store.restoreFromBackup();
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_1));
    }

    @Test
    public void backsUpDefaultValueIfNoBoundExists() {
        store.backupExistingTimestamp(TIMESTAMP_1);
        store.restoreFromBackup();
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_1));
    }

    @Test
    public void firstWriteWinsInPresenceOfMultipleBackups() {
        assertThat(store.backupExistingTimestamp(TIMESTAMP_1)).isEqualTo(TIMESTAMP_1);
        assertThat(store.backupExistingTimestamp(TIMESTAMP_2)).isEqualTo(TIMESTAMP_1);
        store.restoreFromBackup();
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_1));
    }

    @Test
    public void throwsIfRestoringFromUnavailableBackup() {
        assertThatThrownBy(store::restoreFromBackup).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void ignoresInvalidBackupIfDataAvailable() {
        store.storeTimestampBound(Optional.empty(), TIMESTAMP_1);
        store.restoreFromBackup();
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_1));
    }

    @Test
    public void canRestoreTwice() {
        store.backupExistingTimestamp(TIMESTAMP_1);
        store.restoreFromBackup();
        store.restoreFromBackup();
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_1));
    }

    @Test
    public void multipleRestoresDoNotMakeUsGoBackInTime() {
        store.backupExistingTimestamp(TIMESTAMP_1);
        store.restoreFromBackup();
        store.storeTimestampBound(Optional.of(TIMESTAMP_1), TIMESTAMP_2);
        store.restoreFromBackup();
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_2)); // in particular, not TIMESTAMP_1
    }

    @Test
    public void canBackupAndRestoreMultipleTimes() {
        store.storeTimestampBound(Optional.empty(), TIMESTAMP_1);
        assertThat(store.backupExistingTimestamp(TIMESTAMP_3)).isEqualTo(TIMESTAMP_1);
        store.restoreFromBackup();
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_1));
        store.storeTimestampBound(Optional.of(TIMESTAMP_1), TIMESTAMP_2);
        assertThat(store.backupExistingTimestamp(TIMESTAMP_3)).isEqualTo(TIMESTAMP_2);
        store.restoreFromBackup();
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_2));
    }

    @Test
    public void resilientToMultipleConcurrentBackups() {
        executeInParallelOnExecutorService(() -> store.backupExistingTimestamp(TIMESTAMP_1));
        store.restoreFromBackup();
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_1));
    }

    @Test
    public void resilientToMultipleConcurrentRestores() {
        store.storeTimestampBound(Optional.empty(), TIMESTAMP_2);
        executeInParallelOnExecutorService(store::restoreFromBackup);
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_2));
    }

    @Test
    public void resilientToMultipleConcurrentBackupsAndRestores() {
        store.storeTimestampBound(Optional.empty(), TIMESTAMP_3);
        executeInParallelOnExecutorService(() -> {
            if (ThreadLocalRandom.current().nextBoolean()) {
                store.backupExistingTimestamp(TIMESTAMP_2);
            } else {
                store.restoreFromBackup();
            }
        });
        store.restoreFromBackup();
        assertThat(store.getUpperLimit()).isEqualTo(Optional.of(TIMESTAMP_3));
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
