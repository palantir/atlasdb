/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.atlasdb.rocksdb;

import java.util.Optional;

import com.google.auto.service.AutoService;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.SimpleKvsTimestampBoundStore;
import com.palantir.atlasdb.keyvalue.rocksdb.impl.ImmutableWriteOpts;
import com.palantir.atlasdb.keyvalue.rocksdb.impl.RocksDbKeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.versions.AtlasDbVersion;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

@AutoService(AtlasDbFactory.class)
public class RocksDbAtlasDbFactory implements AtlasDbFactory {

    @Override
    public String getType() {
        return "rocksdb";
    }

    @Override
    public RocksDbKeyValueService createRawKeyValueService(KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig) {
        AtlasDbVersion.ensureVersionReported();
        Preconditions.checkArgument(config instanceof RocksDbKeyValueServiceConfig,
                "RocksDbAtlasDbFactory expects a configuration of type RocksDbKeyValueServiceConfig, found %s", config.getClass());
        RocksDbKeyValueServiceConfig rocksDbConfig = (RocksDbKeyValueServiceConfig) config;
        return RocksDbKeyValueService.create(
                rocksDbConfig.dataDir().getAbsolutePath(),
                MoreObjects.firstNonNull(rocksDbConfig.dbOptions(), ImmutableMap.<String, String>of()),
                MoreObjects.firstNonNull(rocksDbConfig.cfOptions(), ImmutableMap.<String, String>of()),
                MoreObjects.firstNonNull(rocksDbConfig.writeOptions(), ImmutableWriteOpts.builder().build()),
                rocksDbConfig.getComparator());
    }

    @Override
    public TimestampService createTimestampService(KeyValueService rawKvs) {
        AtlasDbVersion.ensureVersionReported();
        Preconditions.checkArgument(rawKvs instanceof RocksDbKeyValueService,
                "TimestampService must be created from an instance of RocksDbKeyValueService, found %s", rawKvs.getClass());
        return PersistentTimestampService.create(SimpleKvsTimestampBoundStore.create(rawKvs));
    }

}
