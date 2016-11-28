/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.dbkvs;

import com.google.auto.service.AutoService;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.ConnectionManagerAwareDbKvs;
import com.palantir.atlasdb.keyvalue.dbkvs.timestamp.InDbTimestampBoundStore;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

@AutoService(AtlasDbFactory.class)
public class DbAtlasDbFactory implements AtlasDbFactory {
    public static final String TYPE = "relational";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public KeyValueService createRawKeyValueService(KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig) {
        Preconditions.checkArgument(config instanceof DbKeyValueServiceConfig,
                "DbAtlasDbFactory expects a configuration of type DbKeyValueServiceConfiguration, found %s",
                config.getClass());
        return ConnectionManagerAwareDbKvs.create((DbKeyValueServiceConfig) config);
    }

    @Override
    public TimestampService createTimestampService(KeyValueService rawKvs) {
        Preconditions.checkArgument(rawKvs instanceof ConnectionManagerAwareDbKvs,
                "DbAtlasDbFactory expects a raw kvs of type ConnectionManagerAwareDbKvs, found %s", rawKvs.getClass());
        ConnectionManagerAwareDbKvs dbkvs = (ConnectionManagerAwareDbKvs) rawKvs;

        return PersistentTimestampService.create(InDbTimestampBoundStore.create(
                        dbkvs.getConnectionManager(),
                        AtlasDbConstants.TIMESTAMP_TABLE,
                        dbkvs.getTablePrefix()));
    }
}
