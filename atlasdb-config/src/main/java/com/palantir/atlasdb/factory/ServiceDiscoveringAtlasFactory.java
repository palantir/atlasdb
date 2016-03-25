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

import static java.util.stream.StreamSupport.stream;

import java.util.ServiceLoader;
import java.util.function.Predicate;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.timestamp.TimestampService;

public class ServiceDiscoveringAtlasFactory {
    private static final ServiceLoader<AtlasDbFactory> loader = ServiceLoader.load(AtlasDbFactory.class);

    private final KeyValueServiceConfig config;

    public ServiceDiscoveringAtlasFactory(KeyValueServiceConfig config) {
        this.config = config;
    }

    public KeyValueService createKeyValueService() {
        AtlasDbFactory atlasFactory = stream(loader.spliterator(), false)
                .filter(producesCorrectType())
                .findFirst()
                .get();

        return atlasFactory.createRawKeyValueService(config);
    }

    private Predicate<AtlasDbFactory> producesCorrectType() {
        return factory -> config.type().equalsIgnoreCase(factory.getType());
    }

    public TimestampService createTimestampService(KeyValueService rawKvs) {
        return null;
    }
}
