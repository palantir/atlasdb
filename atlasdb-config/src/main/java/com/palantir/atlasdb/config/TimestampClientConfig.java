/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.config;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@JsonSerialize(as = ImmutableTimestampClientConfig.class)
@JsonDeserialize(as = ImmutableTimestampClientConfig.class)
@Value.Immutable(singleton = true)
public abstract class TimestampClientConfig {
    /**
     * @deprecated this is ignored since timestamp batching is now always on.
     */
    @Value.Parameter
    @Value.Default
    @Deprecated
    public boolean enableTimestampBatching() {
        return true;
    }

    // TODO (jkong): Make timestamp wait intervals configurable.
    // This should ONLY be done once the timestamp client supports nanosecond precision;
    // millisecond precision isn't too useful (realistically it's very unlikely you want to set this beyond
    // 2 ms or so).
}
