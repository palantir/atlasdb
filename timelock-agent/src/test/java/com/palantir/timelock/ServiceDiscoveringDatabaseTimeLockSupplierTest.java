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

package com.palantir.timelock;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.DbTimestampCreationSetting;
import com.palantir.atlasdb.config.ImmutableLeaderConfig;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.atlasdb.keyvalue.api.TimestampSeriesProvider;
import com.palantir.atlasdb.memory.InMemoryAtlasDbConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.timestamp.ManagedTimestampService;
import java.util.function.Supplier;
import org.junit.Test;

public class ServiceDiscoveringDatabaseTimeLockSupplierTest {
    private static final TimestampSeries SERIES = TimestampSeries.of("series");

    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final KeyValueServiceConfig keyValueServiceConfig = new InMemoryAtlasDbConfig();
    private final LeaderConfig leaderConfig = ImmutableLeaderConfig.builder()
            .localServer("me")
            .leaders(ImmutableList.of("me"))
            .quorumSize(1)
            .build();

    private final ServiceDiscoveringDatabaseTimeLockSupplier timeLockSupplier =
            new ServiceDiscoveringDatabaseTimeLockSupplier(metricsManager, keyValueServiceConfig, leaderConfig);

    @Test
    public void canGetTimestampServiceForDifferentSeries() {
        assertThatCode(() -> timeLockSupplier.getManagedTimestampService(
                        DbTimestampCreationSetting.of(AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE, SERIES)))
                .doesNotThrowAnyException();
        assertThatCode(() -> timeLockSupplier.getManagedTimestampService(DbTimestampCreationSetting.of(
                        AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE, TimestampSeries.of("serieses??"))))
                .doesNotThrowAnyException();
        assertThatCode(() -> timeLockSupplier.getManagedTimestampService(DbTimestampCreationSetting.of(
                        AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE, TimestampSeries.of("serien"))))
                .doesNotThrowAnyException();
    }

    @Test
    public void throwsIfGettingTimestampServiceForNonstandardTable() {
        assertCreationOfTimestampServiceNotAllowed(() -> timeLockSupplier.getManagedTimestampService(
                DbTimestampCreationSetting.of(TableReference.createFromFullyQualifiedName("namespace.table"), SERIES)));
        assertCreationOfTimestampServiceNotAllowed(() -> timeLockSupplier.getManagedTimestampService(
                DbTimestampCreationSetting.of(TableReference.createFromFullyQualifiedName("i.cannot"), SERIES)));
        assertCreationOfTimestampServiceNotAllowed(() -> timeLockSupplier.getManagedTimestampService(
                DbTimestampCreationSetting.of(AtlasDbConstants.LEGACY_TIMELOCK_TIMESTAMP_TABLE, SERIES)));
    }

    @Test
    public void canGetTimestampServiceProvider() {
        assertThatCode(() -> timeLockSupplier.getTimestampSeriesProvider(AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE))
                .doesNotThrowAnyException();
    }

    @Test
    public void throwsIfGettingTimestampServiceProviderForNonstandardTable() {
        assertCreationOfTimestampSeriesProviderNotAllowed(
                () -> timeLockSupplier.getTimestampSeriesProvider(AtlasDbConstants.LEGACY_TIMELOCK_TIMESTAMP_TABLE));
        assertCreationOfTimestampSeriesProviderNotAllowed(() ->
                timeLockSupplier.getTimestampSeriesProvider(TableReference.createFromFullyQualifiedName("so.random")));
    }

    private void assertCreationOfTimestampServiceNotAllowed(Supplier<ManagedTimestampService> supplier) {
        assertThatThrownBy(supplier::get)
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("timestamp service")
                .hasMessageContaining("not the normal db timelock timestamp table");
    }

    private void assertCreationOfTimestampSeriesProviderNotAllowed(Supplier<TimestampSeriesProvider> supplier) {
        assertThatThrownBy(supplier::get)
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("timestamp series provider")
                .hasMessageContaining("not the normal db timelock timestamp table");
    }
}
