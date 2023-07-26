/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableDebugThreadInfoConfiguration.class)
@JsonSerialize(as = ImmutableDebugThreadInfoConfiguration.class)
@Value.Immutable
public interface DebugThreadInfoConfiguration {

    @JsonProperty("record-thread-info")
    @Value.Default
    default boolean recordThreadInfo() {
        return false;
    }

    @JsonProperty("thread-info-snapshot-interval-millis")
    @Value.Default
    default long threadInfoSnapshotIntervalMillis() {
        return 300000L;
    }
}
