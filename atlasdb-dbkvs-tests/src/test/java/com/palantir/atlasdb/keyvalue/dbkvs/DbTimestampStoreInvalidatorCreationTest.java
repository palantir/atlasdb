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

import static com.palantir.atlasdb.spi.AtlasDbFactory.NO_OP_FAST_FORWARD_TIMESTAMP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.ServiceDiscoveringAtlasSupplier;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.refreshable.Refreshable;
import com.palantir.timestamp.TimestampBoundStore;
import com.palantir.timestamp.TimestampStoreInvalidator;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class DbTimestampStoreInvalidatorCreationTest {
    private final MetricsManager metrics = MetricsManagers.createForTests();
    private final Optional<LeaderConfig> leaderConfig = Optional.of(mock(LeaderConfig.class));

    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(DbkvsPostgresTestSuite::createKvs);

    private final ConnectionManagerAwareDbKvs kvs = (ConnectionManagerAwareDbKvs) TRM.getDefaultKvs();
    private final TableReference otherTable = TableReference.createWithEmptyNamespace("fooBar");
    private final String prefix = "";

    private final TimestampBoundStore defaultStore = getStore(
            AtlasDbConstants.TIMESTAMP_TABLE,
            DbkvsPostgresTestSuite.getKvsConfig().ddl().tablePrefix());
    private final TimestampBoundStore otherStore = getStore(otherTable, prefix);

    private final InvalidationRunner invalidationRunner =
            new InvalidationRunner(kvs.getConnectionManager(), otherTable, prefix);
    private static final long TIMESTAMP_1 = 12000;

    @Before
    public void setUp() {
        kvs.dropTables(ImmutableSet.of(otherTable, AtlasDbConstants.TIMESTAMP_TABLE));
        invalidationRunner.createTableIfDoesNotExist();
    }

    @Test
    public void canInvalidatorForSingleSeriesTable() {
        TimestampStoreInvalidator timestampStoreInvalidator =
                storeUpperLimitAndGetTimestampStoreInvalidator(Optional.of(otherTable));
        assertThat(timestampStoreInvalidator.backupAndInvalidate()).isEqualTo(TIMESTAMP_1);

        assertBoundNotReadableAfterBeingPoisoned(otherStore);
    }

    @Test
    public void invalidatesDefaultTableForEmptyParameters() {
        TimestampStoreInvalidator timestampStoreInvalidator =
                storeUpperLimitAndGetTimestampStoreInvalidator(Optional.empty());

        assertThat(timestampStoreInvalidator.backupAndInvalidate()).isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);

        assertStoreNotPoisoned(otherStore);
        assertBoundNotReadableAfterBeingPoisoned(defaultStore);
    }

    // utils
    private TimestampBoundStore getStore(TableReference tableReference, String tablePrefix) {
        return InDbTimestampBoundStore.create(kvs.getConnectionManager(), tableReference, tablePrefix);
    }

    private void assertStoreNotPoisoned(TimestampBoundStore store) {
        assertThat(store.getUpperLimit()).isEqualTo(TIMESTAMP_1);
    }

    private void assertBoundNotReadableAfterBeingPoisoned(TimestampBoundStore store) {
        // This timeout is only meant for tests, the server retries for 3 minutes
        TimeLimiter limit = SimpleTimeLimiter.create(Executors.newSingleThreadExecutor());
        assertThatThrownBy(() -> limit.runWithTimeout(store::getUpperLimit, Duration.ofSeconds(1)))
                .isInstanceOf(TimeoutException.class);
    }

    private TimestampStoreInvalidator storeUpperLimitAndGetTimestampStoreInvalidator(
            Optional<TableReference> tableReference) {
        otherStore.storeUpperLimit(TIMESTAMP_1);
        otherStore.getUpperLimit();
        ServiceDiscoveringAtlasSupplier atlasSupplier =
                createAtlasSupplier(DbkvsPostgresTestSuite.getKvsConfig(), tableReference);
        return atlasSupplier.getTimestampStoreInvalidator();
    }

    private ServiceDiscoveringAtlasSupplier createAtlasSupplier(
            KeyValueServiceConfig providedKvsConfig, Optional<TableReference> tableReference) {
        return new ServiceDiscoveringAtlasSupplier(
                metrics,
                providedKvsConfig,
                Refreshable.only(Optional.empty()),
                leaderConfig,
                Optional.empty(),
                tableReference,
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC,
                AtlasDbFactory.THROWING_FRESH_TIMESTAMP_SOURCE);
    }
}
