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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.cassandra.CassandraKeyValueServiceConfigManager;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.common.base.Throwables;
import com.palantir.timestamp.TimestampBoundStore;

public class CassandraTimestampBackupIntegrationTest {
    private static final long INITIAL_VALUE = CassandraTimestampUtils.INITIAL_VALUE;
    private static final long TIMESTAMP_1 = INITIAL_VALUE + 1000;
    private static final long TIMESTAMP_2 = TIMESTAMP_1 + 1000;
    private static final long TIMESTAMP_3 = TIMESTAMP_2 + 1000;

    private static final long TIMEOUT_SECONDS = 5L;
    private static final int POOL_SIZE = 16;

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
        assertThat(backupRunner.backupExistingTimestamp(CassandraTimestampUtils.INITIAL_VALUE))
                .isEqualTo(CassandraTimestampUtils.INITIAL_VALUE);
    }

    @Test
    public void canBackupAlreadyStoredBound() {
        timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(TIMESTAMP_1);
        assertThat(backupRunner.backupExistingTimestamp(TIMESTAMP_2)).isEqualTo(TIMESTAMP_1);
    }

    @Test
    public void cannotReadAfterBackup() {
        backupRunner.backupExistingTimestamp(TIMESTAMP_1);
        assertTimestampNotReadable();
    }

    @Test
    public void canBackupMultipleTimes() {
        assertThat(backupRunner.backupExistingTimestamp(TIMESTAMP_1)).isEqualTo(TIMESTAMP_1);
        assertThat(backupRunner.backupExistingTimestamp(TIMESTAMP_2)).isEqualTo(TIMESTAMP_1);
        assertThat(backupRunner.backupExistingTimestamp(TIMESTAMP_3)).isEqualTo(TIMESTAMP_1);
    }

    @Test
    public void resilientToMultipleConcurrentBackups() {
        executeInParallelOnExecutorService(() -> backupRunner.backupExistingTimestamp(TIMESTAMP_1));
        assertTimestampNotReadable();
    }

    private void assertTimestampNotReadable() {
        assertThatThrownBy(timestampBoundStore::getUpperLimit).isInstanceOf(IllegalArgumentException.class);
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
