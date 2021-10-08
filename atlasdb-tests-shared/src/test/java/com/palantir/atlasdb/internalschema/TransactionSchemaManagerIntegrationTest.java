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

package com.palantir.atlasdb.internalschema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.internalschema.persistence.CoordinationServices;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.timelock.paxos.AbstractTestWithInMemoryTimeLock;
import com.palantir.timestamp.ManagedTimestampService;
import org.junit.Before;
import org.junit.Test;

public class TransactionSchemaManagerIntegrationTest extends AbstractTestWithInMemoryTimeLock {
    private static final long ONE_HUNDRED_MILLION = 100_000_000;

    private ManagedTimestampService timestamps;
    private TransactionSchemaManager manager;

    @Override
    @Before
    public void setUp() {
        super.setUp();
        timestamps = services.getManagedTimestampService();
        manager = createTransactionSchemaManager();
        assertThat(manager.tryInstallNewTransactionsSchemaVersion(1)).isTrue();
    }

    @Test
    public void canForceCoordinations() {
        assertThat(manager.getTransactionsSchemaVersion(337)).isEqualTo(1);
    }

    @Test
    public void newSchemaVersionsCanBeInstalledWithinOneHundredMillionTimestamps() {
        assertThat(manager.tryInstallNewTransactionsSchemaVersion(2)).isTrue();
        fastForwardTimestampByOneHundredMillion();
        assertThat(manager.getTransactionsSchemaVersion(timestamps.getFreshTimestamp()))
                .isEqualTo(2);
    }

    @Test
    public void canSwitchBetweenSchemaVersions() {
        assertThat(manager.tryInstallNewTransactionsSchemaVersion(2)).isTrue();
        fastForwardTimestampByOneHundredMillion();
        assertThat(manager.getTransactionsSchemaVersion(timestamps.getFreshTimestamp()))
                .isEqualTo(2);
        assertThat(manager.tryInstallNewTransactionsSchemaVersion(1)).isTrue();
        fastForwardTimestampByOneHundredMillion();
        assertThat(manager.getTransactionsSchemaVersion(timestamps.getFreshTimestamp()))
                .isEqualTo(1);
    }

    @Test
    public void canFailToInstallNewVersions() {
        TransactionSchemaManager newManager = createTransactionSchemaManager();
        // Always need to seed the default value, if it's not there
        assertThat(newManager.tryInstallNewTransactionsSchemaVersion(5)).isFalse();
        assertThat(newManager.tryInstallNewTransactionsSchemaVersion(5)).isTrue();
    }

    @Test
    public void throwsIfTryingToGetAnImpossibleTimestamp() {
        assertThatThrownBy(() -> manager.getTransactionsSchemaVersion(AtlasDbConstants.STARTING_TS - 3141592))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("was never given out by the timestamp service");
    }

    private TransactionSchemaManager createTransactionSchemaManager() {
        return new TransactionSchemaManager(CoordinationServices.createDefault(
                new InMemoryKeyValueService(true), timestamps, MetricsManagers.createForTests(), false));
    }

    private void fastForwardTimestampByOneHundredMillion() {
        long currentTimestamp = timestamps.getFreshTimestamp();
        timestamps.fastForwardTimestamp(currentTimestamp + ONE_HUNDRED_MILLION);
    }
}
