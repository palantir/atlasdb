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
package com.palantir.atlasdb.cassandra;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraKeyValueService;
import com.palantir.atlasdb.keyvalue.cassandra.CassandraTimestampBoundStore;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

public class CassandraAtlasDbFactory implements AtlasDbFactory {
    
    @Override
    public KeyValueService createRawKeyValueService(KeyValueServiceConfig config) {
        Preconditions.checkArgument(config instanceof CassandraKeyValueServiceConfig,
                "CassandraAtlasDbFactory expects a configuration of type CassandraKeyValueServiceConfig, found %s", config.getClass());
        return createKv((CassandraKeyValueServiceConfig) config);
    }
    
    private static CassandraKeyValueService createKv(CassandraKeyValueServiceConfig config) {
        return CassandraKeyValueService.create(config);
    }

    @Override
    public TimestampService createTimestampService(KeyValueService rawKvs) {
        Preconditions.checkArgument(rawKvs instanceof CassandraKeyValueService, 
                "TimestampService must be created from an instance of CassandraKeyValueService, found %s", rawKvs.getClass());
        return PersistentTimestampService.create(CassandraTimestampBoundStore.create((CassandraKeyValueService) rawKvs));
    }
    
    @Override
    public String getType() {
        return CassandraKeyValueServiceConfig.TYPE;
    }

}
