/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.kafka;

import com.google.auto.service.AutoService;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.kafka.KafkaKeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.timestamp.TimestampService;

@AutoService(AtlasDbFactory.class)
public class KafkaAtlasDbFactory implements AtlasDbFactory {
    @Override
    public String getType() {
        return KafkaKeyValueServiceConfig.TYPE;
    }

    @Override
    public KeyValueService createRawKeyValueService(KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig) {
        Preconditions.checkArgument(config instanceof KafkaKeyValueServiceConfig,
                "KafkaAtlasDbFactory expects a configuration of type"
                        + "KafkaKeyValueServiceConfig, found %s", config.getClass());
        return KafkaKeyValueService.create((KafkaKeyValueServiceConfig) config);
    }

    @Override
    public TimestampService createTimestampService(KeyValueService rawKvs) {
        throw new UnsupportedOperationException("Not supported on this KVS");
    }
}
