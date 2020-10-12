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

import java.util.ServiceLoader;

import com.palantir.atlasdb.spi.AtlasDbFactory;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class AtlasDbServiceDiscovery {
    private AtlasDbServiceDiscovery() {
        // util
    }

    public static AtlasDbFactory createAtlasFactoryOfCorrectType(KeyValueServiceConfig config) {
        for (AtlasDbFactory factory : ServiceLoader.load(AtlasDbFactory.class)) {
            if (config.type().equalsIgnoreCase(factory.getType())) {
                return factory;
            }
        }
        throw new SafeIllegalStateException("No atlas provider for the configured type could be found. "
                + "Ensure that the implementation of the AtlasDbFactory is annotated "
                + "@AutoService(AtlasDbFactory.class) and that it is on your classpath.",
                SafeArg.of("type", config.type()));
    }
}
