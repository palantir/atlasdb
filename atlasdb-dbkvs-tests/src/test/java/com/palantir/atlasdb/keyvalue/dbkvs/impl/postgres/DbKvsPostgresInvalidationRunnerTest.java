/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres;

import static com.palantir.atlasdb.spi.AtlasDbFactory.NO_OP_FAST_FORWARD_TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.InvalidationRunner;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.timestamp.TimestampBoundStore;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class DbKvsPostgresInvalidationRunnerTest {
    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(DbKvsPostgresTestSuite::createKvs);

    private final ConnectionManagerAwareDbKvs kvs = (ConnectionManagerAwareDbKvs) TRM.getDefaultKvs();
    private final TimestampBoundStore store = getStore();
    private final InvalidationRunner invalidationRunner = new InvalidationRunner(
            kvs.getConnectionManager(),
            AtlasDbConstants.TIMESTAMP_TABLE,
            DbKvsPostgresTestSuite.getKvsConfig().ddl().tablePrefix());
    private static final long TIMESTAMP_1 = 12000;

    @Before
    public void setUp() {
        kvs.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
        invalidationRunner.createTableIfDoesNotExist();
    }

    @Test
    public void returnsDefaultTsWhenTableIsEmpty() {
        assertThat(invalidationRunner.ensureInDbStoreIsPoisonedAndGetLastAllocatedTimestamp())
                .isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
    }

    @Test
    public void poisonsEmptyTableAndReturnsStoredBound() {
        store.getUpperLimit();
        store.storeUpperLimit(TIMESTAMP_1);
        assertThat(invalidationRunner.ensureInDbStoreIsPoisonedAndGetLastAllocatedTimestamp())
                .isEqualTo(TIMESTAMP_1);
    }

    @Test
    public void cannotReadAfterBeingPoisoned() {
        invalidationRunner.ensureInDbStoreIsPoisonedAndGetLastAllocatedTimestamp();
        assertBoundNotReadableAfterBeingPoisoned();
    }

    @Test
    public void poisoningMultipleTimesIsAllowed() {
        store.storeUpperLimit(TIMESTAMP_1);
        store.getUpperLimit();
        assertThat(invalidationRunner.ensureInDbStoreIsPoisonedAndGetLastAllocatedTimestamp())
                .isEqualTo(TIMESTAMP_1);
        assertThat(invalidationRunner.ensureInDbStoreIsPoisonedAndGetLastAllocatedTimestamp())
                .isEqualTo(TIMESTAMP_1);
        assertThat(invalidationRunner.ensureInDbStoreIsPoisonedAndGetLastAllocatedTimestamp())
                .isEqualTo(TIMESTAMP_1);
    }

    @Test
    public void poisoningEmptyTableMultipleTimesIsAllowed() {
        assertThat(invalidationRunner.ensureInDbStoreIsPoisonedAndGetLastAllocatedTimestamp())
                .isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
        assertThat(invalidationRunner.ensureInDbStoreIsPoisonedAndGetLastAllocatedTimestamp())
                .isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
        assertThat(invalidationRunner.ensureInDbStoreIsPoisonedAndGetLastAllocatedTimestamp())
                .isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
        assertBoundNotReadableAfterBeingPoisoned();
    }

    public TimestampBoundStore getStore() {
        return InDbTimestampBoundStore.create(
                kvs.getConnectionManager(),
                AtlasDbConstants.TIMESTAMP_TABLE,
                DbKvsPostgresTestSuite.getKvsConfig().ddl().tablePrefix());
    }

    private void assertBoundNotReadableAfterBeingPoisoned() {
        // This timeout is only meant for tests, the server retries for 3 minutes
        TimeLimiter limit = SimpleTimeLimiter.create(Executors.newSingleThreadExecutor());
        assertThatThrownBy(() -> limit.runWithTimeout(store::getUpperLimit, Duration.ofSeconds(1)))
                .isInstanceOf(TimeoutException.class);
    }
}
