/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static com.palantir.atlasdb.spi.AtlasDbFactory.NO_OP_FAST_FORWARD_TIMESTAMP;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.exception.PalantirSqlException;
import com.palantir.timestamp.TimestampBoundStore;

public class DbKvsPostgresInvalidationRunnerTest {
    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(DbkvsPostgresTestSuite::createKvs);

    private final ConnectionManagerAwareDbKvs kvs = (ConnectionManagerAwareDbKvs) TRM.getDefaultKvs();
    private final TimestampBoundStore store = getStore();
    private final InvalidationRunner invalidationRunner = new InvalidationRunner(kvs.getConnectionManager());
    private static final long TIMESTAMP_1 = 12000;

    @Before
    public void setUp() {
        kvs.dropTable(AtlasDbConstants.TIMELOCK_TIMESTAMP_TABLE);
        invalidationRunner.createTableIfDoesNotExist();
    }

    @Test
    public void returnsDefaultTsWhenTableIsEmpty() {
        assertThat(invalidationRunner.getLastAllocatedTimestampAndPoisonInDbStore())
                .isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
    }

    @Test
    public void poisonsEmptyTableAndReturnsStoredBound() {
        store.getUpperLimit();
        store.storeUpperLimit(TIMESTAMP_1);
        assertThat(invalidationRunner.getLastAllocatedTimestampAndPoisonInDbStore()).isEqualTo(TIMESTAMP_1);
    }

    @Test
    public void cannotReadAfterBeingPoisoned() {
        invalidationRunner.getLastAllocatedTimestampAndPoisonInDbStore();
        assertBoundNotReadable();
    }

    @Test
    public void poisoningMultipleTimesIsAllowed() {
        store.storeUpperLimit(TIMESTAMP_1);
        store.getUpperLimit();
        assertThat(invalidationRunner.getLastAllocatedTimestampAndPoisonInDbStore()).isEqualTo(TIMESTAMP_1);
        assertThat(invalidationRunner.getLastAllocatedTimestampAndPoisonInDbStore()).isEqualTo(TIMESTAMP_1);
        assertThat(invalidationRunner.getLastAllocatedTimestampAndPoisonInDbStore()).isEqualTo(TIMESTAMP_1);
    }

    @Test
    public void poisoningEmptyTableMultipleTimesIsAllowed() {
        assertThat(invalidationRunner.getLastAllocatedTimestampAndPoisonInDbStore())
                .isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
        assertThat(invalidationRunner.getLastAllocatedTimestampAndPoisonInDbStore())
                .isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
        assertThat(invalidationRunner.getLastAllocatedTimestampAndPoisonInDbStore())
                .isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
        assertBoundNotReadable();
    }

    public InDbTimestampBoundStore getStore() {
        return InDbTimestampBoundStore.create(
                kvs.getConnectionManager(),
                AtlasDbConstants.TIMELOCK_TIMESTAMP_TABLE,
                DbkvsPostgresTestSuite.getKvsConfig().ddl().tablePrefix());
    }

    private void assertBoundNotReadable() {
        assertThatThrownBy(store::getUpperLimit).isInstanceOf(PalantirSqlException.class);
    }
}
