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
package com.palantir.atlasdb.factory;

import java.util.ArrayList;
import java.util.List;

import org.jmock.Mockery;

import com.google.auto.service.AutoService;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.timestamp.TimestampService;

@AutoService(AtlasDbFactory.class)
public class AutoServiceAnnotatedAtlasDbFactory implements AtlasDbFactory {
    public static final String TYPE = "not-a-real-db";

    private static final Mockery context = new Mockery();
    private static final KeyValueService keyValueService = context.mock(KeyValueService.class);
    private static List<TimestampService> nextTimestampServices = new ArrayList<>();


    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public KeyValueService createRawKeyValueService(KeyValueServiceConfig config, Optional<LeaderConfig> leaderConfig) {
        return keyValueService;
    }

    @Override
    public TimestampService createTimestampService(KeyValueService rawKvs) {
        return nextTimestampServices.remove(0);
    }

    public static void nextTimestampServiceToReturn(TimestampService... timestampServices) {
        nextTimestampServices = Lists.newArrayList(timestampServices);
    }
}
