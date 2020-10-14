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
import static org.mockito.Mockito.mock;

import static com.palantir.atlasdb.spi.AtlasDbFactory.NO_OP_FAST_FORWARD_TIMESTAMP;

import java.util.Optional;

import org.junit.ClassRule;
import org.junit.Test;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.DbTimestampCreationSetting;
import com.palantir.atlasdb.config.DbTimestampCreationSettings;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.ServiceDiscoveringAtlasSupplier;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.impl.TestResourceManager;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.exception.PalantirSqlException;
import com.palantir.timestamp.TimestampBoundStore;
import com.palantir.timestamp.TimestampStoreInvalidator;

public class DbTimestampStoreInvalidatorCreationTest {
    private final MetricsManager metrics = MetricsManagers.createForTests();
    private final Optional<LeaderConfig> leaderConfig = Optional.of(mock(LeaderConfig.class));

    @ClassRule
    public static final TestResourceManager TRM = new TestResourceManager(DbkvsPostgresTestSuite::createKvs);

    private final ConnectionManagerAwareDbKvs kvs = (ConnectionManagerAwareDbKvs) TRM.getDefaultKvs();
    private final TableReference otherTable = TableReference.createWithEmptyNamespace("fooBar");
    private final TimestampBoundStore otherStore = getStore(otherTable,"");
    private static final long TIMESTAMP_1 = 12000;


    @Test
    public void canNoInvalidatorForMultiSeries() {
        otherStore.storeUpperLimit(TIMESTAMP_1);
        otherStore.getUpperLimit();

        ServiceDiscoveringAtlasSupplier atlasSupplier = createAtlasSupplier(
                DbkvsPostgresTestSuite.getKvsConfig(),
                Optional.of(DbTimestampCreationSettings.multipleSeries(Optional.of(otherTable), TimestampSeries.of("blah"))));
        TimestampStoreInvalidator timestampStoreInvalidator = atlasSupplier.getTimestampStoreInvalidator();
        assertThat(timestampStoreInvalidator.backupAndInvalidate()).isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
    }

    @Test
    public void canInvalidatorForSingleSeries() {
        otherStore.storeUpperLimit(12027);
        otherStore.getUpperLimit();
        ServiceDiscoveringAtlasSupplier atlasSupplier = createAtlasSupplier(
                DbkvsPostgresTestSuite.getKvsConfig(),
                Optional.of(DbTimestampCreationSettings.singleSeries(Optional.of(otherTable))));
        TimestampStoreInvalidator timestampStoreInvalidator = atlasSupplier.getTimestampStoreInvalidator();
        assertThat(timestampStoreInvalidator.backupAndInvalidate()).isEqualTo(12027);
    }

    @Test
    public void createsInvalidatorForDefaultSingleSeries() {
        otherStore.storeUpperLimit(TIMESTAMP_1);
        otherStore.getUpperLimit();
        ServiceDiscoveringAtlasSupplier atlasSupplier = createAtlasSupplier(
                DbkvsPostgresTestSuite.getKvsConfig(),
                Optional.of(DbTimestampCreationSettings.singleSeries(Optional.empty())));
        TimestampStoreInvalidator timestampStoreInvalidator = atlasSupplier.getTimestampStoreInvalidator();
        assertThat(timestampStoreInvalidator.backupAndInvalidate()).isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
    }

    @Test
    public void createsDefaultInvalidatorForEmptyParameters() {
        otherStore.storeUpperLimit(TIMESTAMP_1);
        otherStore.getUpperLimit();
        ServiceDiscoveringAtlasSupplier atlasSupplier = createAtlasSupplier(
                DbkvsPostgresTestSuite.getKvsConfig(),
                Optional.empty());
        TimestampStoreInvalidator timestampStoreInvalidator = atlasSupplier.getTimestampStoreInvalidator();
        assertThat(timestampStoreInvalidator.backupAndInvalidate()).isEqualTo(NO_OP_FAST_FORWARD_TIMESTAMP);
    }


    public InDbTimestampBoundStore getStore(TableReference tableReference, String tablePrefix) {
        return InDbTimestampBoundStore.create(
                kvs.getConnectionManager(),
                tableReference,
                tablePrefix);
    }

    private void assertBoundNotReadableAfterBeingPoisoned(TimestampBoundStore store) {
        assertThatThrownBy(store::getUpperLimit).isInstanceOf(PalantirSqlException.class);
    }

    private ServiceDiscoveringAtlasSupplier createAtlasSupplier(KeyValueServiceConfig providedKvsConfig,
            Optional<DbTimestampCreationSetting> dbTimestampCreationParameters) {
        return new ServiceDiscoveringAtlasSupplier(metrics,
                providedKvsConfig,
                Optional::empty,
                leaderConfig,
                Optional.empty(),
                dbTimestampCreationParameters,
                AtlasDbConstants.DEFAULT_INITIALIZE_ASYNC,
                AtlasDbFactory.THROWING_FRESH_TIMESTAMP_SOURCE);
    }


}
