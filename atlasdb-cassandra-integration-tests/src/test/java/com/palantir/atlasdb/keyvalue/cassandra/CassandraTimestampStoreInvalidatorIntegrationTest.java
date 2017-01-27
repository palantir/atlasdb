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
import com.palantir.timestamp.TimestampStoreInvalidator;

public class CassandraTimestampStoreInvalidatorIntegrationTest {
    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraTimestampStoreInvalidatorIntegrationTest.class)
            .with(new CassandraContainer());

    private static final long ZERO = 0L;
    private static final long ONE_MILLION = 1000000L;
    private static final int POOL_SIZE = 16;
    private static final int TIMEOUT_SECONDS = 5;

    private final CassandraKeyValueService kv = CassandraKeyValueService.create(
            CassandraKeyValueServiceConfigManager.createSimpleManager(CassandraContainer.KVS_CONFIG),
            CassandraContainer.LEADER_CONFIG);
    private final TimestampStoreInvalidator invalidator = new CassandraTimestampStoreInvalidator(kv);

    private TimestampBoundStore timestampBoundStore;
    private ExecutorService executorService;

    @Before
    public void setUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        timestampBoundStore = CassandraTimestampBoundStore.create(kv);
        executorService = Executors.newFixedThreadPool(POOL_SIZE);
    }

    @After
    public void close() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        kv.close();
    }

    @Test
    public void canInvalidateTimestampTableIfItAlreadyExistsWithData() {
        timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(ONE_MILLION);
        invalidateAndThenCheckCannotReadBound();
    }

    @Test
    public void canInvalidateTimestampTableIfItAlreadyExistsWithoutData() {
        invalidateAndThenCheckCannotReadBound();
    }

    @Test
    public void canInvalidateTimestampTableIfItDoesNotExistYet() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        invalidateAndThenCheckCannotReadBound();
    }

    @Test
    public void invalidateIsIdempotent() {
        invalidator.invalidateTimestampStore();
        invalidateAndThenCheckCannotReadBound();
    }

    @Test
    public void resilientToMultipleConcurrentInvalidations() {
        executeInParallelOnExecutorService(this::invalidateAndThenCheckCannotReadBound);
        revalidateAndThenCheckCanReadBound(ZERO);
    }

    @Test
    public void cannotGoBackInTimeWithRevalidation() {
        invalidateAndThenCheckCannotReadBound();
        revalidateAndThenCheckCanReadBound(ZERO);
        timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(ONE_MILLION);
        revalidateAndThenCheckCanReadBound(ONE_MILLION); // in particular, not ZERO
    }

    @Test
    public void canReadTimestampTableAfterRevalidation() {
        timestampBoundStore.getUpperLimit();
        timestampBoundStore.storeUpperLimit(ONE_MILLION);
        invalidateAndThenCheckCannotReadBound();
        revalidateAndThenCheckCanReadBound(ONE_MILLION);
    }

    @Test
    public void resilientToMultipleConcurrentRevalidations() {
        invalidateAndThenCheckCannotReadBound();
        executeInParallelOnExecutorService(() -> revalidateAndThenCheckCanReadBound(ZERO));
    }

    private void invalidateAndThenCheckCannotReadBound() {
        invalidator.invalidateTimestampStore();
        assertThatThrownBy(timestampBoundStore::getUpperLimit).isInstanceOf(IllegalArgumentException.class);
    }

    private void revalidateAndThenCheckCanReadBound(long expected) {
        invalidator.revalidateTimestampStore();
        assertThat(timestampBoundStore.getUpperLimit()).isEqualTo(expected);
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
