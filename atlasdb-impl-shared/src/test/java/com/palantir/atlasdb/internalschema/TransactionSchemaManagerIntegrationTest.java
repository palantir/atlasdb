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

import com.palantir.atlasdb.coordination.CoordinationService;
import com.palantir.atlasdb.coordination.CoordinationServiceImpl;
import com.palantir.atlasdb.coordination.keyvalue.KeyValueServiceCoordinationStore;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.internalschema.persistence.VersionedInternalSchemaMetadata;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.remoting3.ext.jackson.ObjectMappers;
import com.palantir.timestamp.InMemoryTimestampService;

public class TransactionSchemaManagerIntegrationTest {
    private static final long ONE_HUNDRED_MILLION = 100_000_000;

    private final InMemoryTimestampService timestamps = new InMemoryTimestampService();
    private final CoordinationServiceImpl<VersionedInternalSchemaMetadata> rawCoordinationService
            = new CoordinationServiceImpl<>(KeyValueServiceCoordinationStore.create(
                    ObjectMappers.newServerObjectMapper(),
                    new InMemoryKeyValueService(true),
                    PtBytes.toBytes("blablabla"),
                    timestamps::getFreshTimestamp,
                    VersionedInternalSchemaMetadata.class));
    private final CoordinationService<InternalSchemaMetadata> actualCoordinationService
            = CoordinationServices.wrapHidingVersionSerialization(rawCoordinationService);
    private final TransactionSchemaManager manager = new TransactionSchemaManager(
            actualCoordinationService);

    @Before
    public void setUp() {
        new InternalSchemaMetadataInitializer(actualCoordinationService)
                .ensureInternalSchemaMetadataInitialized();
    }

    @Test
    public void canForceCoordinations() {
        assertThat(manager.getTransactionsSchemaVersion(337)).isEqualTo(1);
    }

    @Test
    public void newSchemaVersionsCanBeInstalledWithinOneHundredMillionTimestamps() {
        manager.tryInstallNewTransactionsSchemaVersion(2);
        fastForwardTimestampByOneHundredMillion();
        assertThat(manager.getTransactionsSchemaVersion(timestamps.getFreshTimestamp())).isEqualTo(2);
    }

    @Test
    public void canSwitchBetweenSchemaVersions() {
        manager.tryInstallNewTransactionsSchemaVersion(2);
        fastForwardTimestampByOneHundredMillion();
        assertThat(manager.getTransactionsSchemaVersion(timestamps.getFreshTimestamp())).isEqualTo(2);
        manager.tryInstallNewTransactionsSchemaVersion(1);
        fastForwardTimestampByOneHundredMillion();
        assertThat(manager.getTransactionsSchemaVersion(timestamps.getFreshTimestamp())).isEqualTo(1);
    }

    private void fastForwardTimestampByOneHundredMillion() {
        long currentTimestamp = timestamps.getFreshTimestamp();
        timestamps.fastForwardTimestamp(currentTimestamp + ONE_HUNDRED_MILLION);
    }
}
