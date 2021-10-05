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

package com.palantir.atlasdb.factory;

import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.timestamp.DbTimeLockFactory;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.ServiceLoader;
import java.util.function.Function;

public final class AtlasDbServiceDiscovery {
    private AtlasDbServiceDiscovery() {
        // util
    }

    public static <T extends KeyValueServiceConfig> AtlasDbFactory<T> createAtlasFactoryOfCorrectType(
            KeyValueServiceConfig config) {
        return createAtlasDbServiceOfCorrectType(config, AtlasDbFactory::getType, AtlasDbFactory.class);
    }

    public static DbTimeLockFactory createDbTimeLockFactoryOfCorrectType(KeyValueServiceConfig config) {
        return createAtlasDbServiceOfCorrectType(config, DbTimeLockFactory::getType, DbTimeLockFactory.class);
    }

    private static <T> T createAtlasDbServiceOfCorrectType(
            KeyValueServiceConfig config, Function<T, String> typeExtractor, Class<T> clazz) {
        for (T element : ServiceLoader.load(clazz)) {
            if (config.type().equalsIgnoreCase(typeExtractor.apply(element))) {
                return element;
            }
        }
        throw new SafeIllegalStateException(
                "No atlas provider for the configured type could be found. "
                        + "Ensure that the implementation of the AtlasDbFactory is annotated "
                        + "@AutoService with a suitable class as parameter and that it is on your classpath.",
                SafeArg.of("class", clazz),
                SafeArg.of("type", config.type()));
    }
}
