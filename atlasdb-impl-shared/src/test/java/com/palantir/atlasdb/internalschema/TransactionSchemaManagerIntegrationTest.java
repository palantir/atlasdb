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

package com.palantir.atlasdb.internalschema;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.palantir.atlasdb.coordination.CoordinationServiceImpl;
import com.palantir.atlasdb.coordination.keyvalue.KeyValueServiceCoordinationStore;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.remoting3.ext.jackson.ObjectMappers;
import com.palantir.timestamp.InMemoryTimestampService;

public class TransactionSchemaManagerIntegrationTest {
    private static final long ONE_HUNDRED_MILLION = 100_000_000;

    private final InMemoryTimestampService timestamps = new InMemoryTimestampService();
    private final CoordinationServiceImpl<InternalSchemaMetadata> coordinationService = new CoordinationServiceImpl<>(
            KeyValueServiceCoordinationStore.create(
                    ObjectMappers.newServerObjectMapper().registerModule(new GuavaModule()),
                    new InMemoryKeyValueService(true),
                    PtBytes.toBytes("blablabla"),
                    timestamps::getFreshTimestamp,
                    InternalSchemaMetadata.class));
    private final TransactionSchemaManager manager = new TransactionSchemaManager(coordinationService);

    @Before
    public void setUp() {
        new InternalSchemaMetadataInitializer(coordinationService, timestamps)
                .ensureInternalSchemaMetadataInitialized();
    }

    @Test
    public void canForceCoordinations() {
        assertThat(manager.getTransactionsSchemaVersion(337)).isEqualTo(1);
    }

    @Test
    public void newSchemaVersionsCanBeInstalledWithinOneHundredMillionTimestamps() {
        manager.installNewTransactionsSchemaVersion(2);
        fastForwardTimestampByOneHundredMillion();
        assertThat(manager.getTransactionsSchemaVersion(timestamps.getFreshTimestamp())).isEqualTo(2);
    }

    @Test
    public void canSwitchBetweenSchemaVersions() {
        manager.installNewTransactionsSchemaVersion(2);
        fastForwardTimestampByOneHundredMillion();
        assertThat(manager.getTransactionsSchemaVersion(timestamps.getFreshTimestamp())).isEqualTo(2);
        manager.installNewTransactionsSchemaVersion(1);
        fastForwardTimestampByOneHundredMillion();
        assertThat(manager.getTransactionsSchemaVersion(timestamps.getFreshTimestamp())).isEqualTo(1);
    }

    private void fastForwardTimestampByOneHundredMillion() {
        long currentTimestamp = timestamps.getFreshTimestamp();
        timestamps.fastForwardTimestamp(currentTimestamp + ONE_HUNDRED_MILLION);
    }
}
