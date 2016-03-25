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

import org.jmock.Mockery;

import com.google.auto.service.AutoService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.timestamp.TimestampService;

@AutoService(AtlasDbFactory.class)
public class DummyAtlasDbFactory implements AtlasDbFactory {
    public static final String TYPE = "not-a-real-db";

    private static final Mockery context = new Mockery();
    private static final KeyValueService keyValueService = context.mock(KeyValueService.class);
    private static final TimestampService timestampService = context.mock(TimestampService.class);


    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public KeyValueService createRawKeyValueService(KeyValueServiceConfig config) {
        return keyValueService;
    }

    @Override
    public TimestampService createTimestampService(KeyValueService rawKvs) {
        return timestampService;
    }
}
