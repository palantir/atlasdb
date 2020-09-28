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

package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.palantir.atlasdb.keyvalue.dbkvs.DbkvsPostgresTestSuite;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;

public class PostgresPhysicalDbBoundStoreTest {
    private static final long LIMIT_1 = 55L;
    private static final long LIMIT_3 = 88L;
    private static final long LIMIT_2 = 77L;

    private PhysicalDbBoundStore dbBoundStore;
    private String client;

    @Before
    public void setUp() {
        ConnectionManagerAwareDbKvs kvs = DbkvsPostgresTestSuite.createKvs();
        client = UUID.randomUUID().toString();
        dbBoundStore = PostgresPhysicalDbBoundStore.create(kvs.getConnectionManager(), client);
        dbBoundStore.createTimestampTable();
    }

    @Test
    public void creatingTheTimestampTableAgainIsAllowed() {
        assertThatCode(() -> dbBoundStore.createTimestampTable()).doesNotThrowAnyException();
    }

    @Test
    public void canInitializeBound() {
        assertThat(dbBoundStore.initialize(LIMIT_1)).isTrue();
        assertThat(dbBoundStore.read()).hasValue(LIMIT_1);
    }

    @Test
    public void cannotInitializeMoreThanOnce() {
        assertThat(dbBoundStore.initialize(LIMIT_1)).isTrue();
        assertThat(dbBoundStore.initialize(LIMIT_1)).isFalse();
        assertThat(dbBoundStore.initialize(LIMIT_1)).isFalse();
    }

    @Test
    public void canSuccessfullyCas() {
        assertThat(dbBoundStore.initialize(LIMIT_1)).isTrue();
        assertThat(dbBoundStore.cas(LIMIT_2, LIMIT_1)).isTrue();
        assertThat(dbBoundStore.read()).hasValue(LIMIT_2);
    }

    @Test
    public void casWithWrongValueIsNoOp() {
        assertThat(dbBoundStore.initialize(LIMIT_1)).isTrue();
        assertThat(dbBoundStore.cas(LIMIT_3, LIMIT_2)).isFalse();
        assertThat(dbBoundStore.read()).hasValue(LIMIT_1);
    }

    @Test
    public void readFromEmptyTableIsEmpty() {
        assertThat(dbBoundStore.read()).isEmpty();
    }

    @Test
    public void casFromEmptyTableFails() {
        assertThat(dbBoundStore.cas(LIMIT_2, LIMIT_1)).isFalse();
        assertThat(dbBoundStore.read()).isEmpty();
    }

    @Test
    public void casMultipleTimesAllowed() {
        assertThat(dbBoundStore.initialize(LIMIT_1)).isTrue();
        assertThat(dbBoundStore.cas(LIMIT_2, LIMIT_1)).isTrue();
        assertThat(dbBoundStore.cas(LIMIT_3, LIMIT_2)).isTrue();
        assertThat(dbBoundStore.read()).hasValue(LIMIT_3);
    }
}