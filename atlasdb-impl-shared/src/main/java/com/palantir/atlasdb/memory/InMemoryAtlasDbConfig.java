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
package com.palantir.atlasdb.memory;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.auto.service.AutoService;
import com.palantir.atlasdb.spi.KeyValueServiceConfig;

@JsonTypeName(InMemoryAtlasDbConfig.TYPE)
@AutoService(KeyValueServiceConfig.class)
public final class InMemoryAtlasDbConfig implements KeyValueServiceConfig {
    public static final String TYPE = "memory";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public boolean equals(Object other) {
        return this == other || (other != null && this.getClass() == other.getClass());
    }

    @Override
    public int hashCode() {
        return InMemoryAtlasDbConfig.class.hashCode();
    }

    @Override
    @JsonIgnore
    public Optional<String> namespace() {
        return Optional.of("test");
    }
}
