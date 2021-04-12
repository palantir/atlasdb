/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.palantir.atlasdb.spi.KeyValueServiceRuntimeConfig;
import com.palantir.nexus.db.pool.config.MaskedValue;
import org.immutables.value.Value;

@AutoService(KeyValueServiceRuntimeConfig.class)
@JsonSerialize(as = ImmutableDbKeyValueServiceRuntimeConfig.class)
@JsonDeserialize(as = ImmutableDbKeyValueServiceRuntimeConfig.class)
@Value.Immutable
public abstract class DbKeyValueServiceRuntimeConfig implements KeyValueServiceRuntimeConfig {

    @Override
    public String type() {
        return DbAtlasDbFactory.TYPE;
    }

    public abstract MaskedValue getDbPassword();
}
