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

package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TimestampSeries;
import com.palantir.atlasdb.keyvalue.api.TimestampSeriesProvider;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.postgres.DbKvsPostgresTestSuite;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.MultiSequenceTimestampSeriesProvider;
import com.palantir.atlasdb.timestamp.AbstractDbMultiSeriesTimestampPersistenceTest;
import com.palantir.timestamp.TimestampBoundStore;

public abstract class DbKvsMultiSeriesTimestampPersistenceTest extends AbstractDbMultiSeriesTimestampPersistenceTest {
    protected abstract ConnectionManagerAwareDbKvs getKeyValueService();

    @Override
    protected TimestampBoundStore createSingleSeriesTimestampBoundStore() {
        return InDbTimestampBoundStore.create(
                getKeyValueService().getConnectionManager(),
                AtlasDbConstants.TIMESTAMP_TABLE,
                DbKvsPostgresTestSuite.getKvsConfig().ddl().tablePrefix());
    }

    @Override
    protected TimestampBoundStore createMultiSeriesTimestampBoundStore(TimestampSeries series) {
        return InDbTimestampBoundStore.createForMultiSeries(
                getKeyValueService().getConnectionManager(), AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE, series);
    }

    @Override
    protected TimestampSeriesProvider createTimestampSeriesProvider() {
        return MultiSequenceTimestampSeriesProvider.create(
                getKeyValueService(), AtlasDbConstants.DB_TIMELOCK_TIMESTAMP_TABLE, false);
    }
}
