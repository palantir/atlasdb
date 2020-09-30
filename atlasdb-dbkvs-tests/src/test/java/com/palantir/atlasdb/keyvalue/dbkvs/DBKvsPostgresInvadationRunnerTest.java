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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.exception.PalantirSqlException;
import com.palantir.timestamp.TimestampBoundStore;

public class DBKvsPostgresInvadationRunnerTest {
    private final ConnectionManagerAwareDbKvs kvs = DbkvsPostgresTestSuite.createKvs();
    private final TimestampBoundStore store = getStore();
    private final InvalidationRunner invalidationRunner = new InvalidationRunner(kvs.getConnectionManager());
    private static final long TIMESTAMP_1 = 12000;

    @Before
    public void setUp() throws Exception {
        kvs.dropTable(AtlasDbConstants.TIMELOCK_TIMESTAMP_TABLE);
        invalidationRunner.createTableIfDoesNotExist();
    }

    public InDbTimestampBoundStore getStore() {
        return InDbTimestampBoundStore.create(
                kvs.getConnectionManager(),
                AtlasDbConstants.TIMELOCK_TIMESTAMP_TABLE,
                DbkvsPostgresTestSuite.getKvsConfig().ddl().tablePrefix());
    }

    @After
    public void tearDown() throws Exception {
        kvs.close();
    }

    @Test
    public void poisonsEmptyTableAndReturnsNoOpTs() {
        assertThat(invalidationRunner.getLastAllocatedAndPoison()).isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
    }

    @Test
    public void poisonsEmptyTableAndReturnsStoredBound() {
        store.getUpperLimit();
        store.storeUpperLimit(TIMESTAMP_1);
        assertThat(invalidationRunner.getLastAllocatedAndPoison()).isEqualTo(TIMESTAMP_1);
    }

    @Test
    public void cannotReadAfterBeingPoisoned() {
        invalidationRunner.getLastAllocatedAndPoison();
        assertBoundNotReadable();
    }

    @Test
    public void poisoningMultipleTimesIsAllowed() {
        store.storeUpperLimit(12000);
        store.getUpperLimit();
        invalidationRunner.createTableIfDoesNotExist();
        invalidationRunner.getLastAllocatedAndPoison();
        invalidationRunner.getLastAllocatedAndPoison();
        invalidationRunner.getLastAllocatedAndPoison();
    }

    @Test
    public void poisoningEmptyTableMultipleTimesIsAllowed() {
        assertThat(invalidationRunner.getLastAllocatedAndPoison()).isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
        assertThat(invalidationRunner.getLastAllocatedAndPoison()).isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
        assertThat(invalidationRunner.getLastAllocatedAndPoison()).isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
        assertBoundNotReadable();
    }

    private void assertBoundNotReadable() {
        assertThatThrownBy(store::getUpperLimit).isInstanceOf(PalantirSqlException.class);
    }
}
