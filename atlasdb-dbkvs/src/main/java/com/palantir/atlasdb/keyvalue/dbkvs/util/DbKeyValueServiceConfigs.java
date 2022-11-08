/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs.util;

import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceConfig;
import com.palantir.atlasdb.keyvalue.dbkvs.DbKeyValueServiceRuntimeConfig;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.Optional;

public final class DbKeyValueServiceConfigs {
    private DbKeyValueServiceConfigs() {
        // Utility
    }

    public static DbKeyValueServiceConfig toDbKeyValueServiceConfig(KeyValueServiceConfig config) {
        Preconditions.checkArgument(
                config instanceof DbKeyValueServiceConfig,
                "[Unexpected configuration] | DbAtlasDbFactory expects a configuration of type "
                        + "DbKeyValueServiceConfiguration.",
                SafeArg.of("configurationClass", config.getClass()));
        return (DbKeyValueServiceConfig) config;
    }

    public static Optional<DbKeyValueServiceRuntimeConfig> tryCastToDbKeyValueServiceRuntimeConfig(
            KeyValueServiceRuntimeConfig runtimeConfig) {
        if (runtimeConfig instanceof DbKeyValueServiceRuntimeConfig) {
            return Optional.of((DbKeyValueServiceRuntimeConfig) runtimeConfig);
        }
        return Optional.empty();
    }
}
