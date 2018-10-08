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
package com.palantir.atlasdb.qos.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonDeserialize(as = ImmutableQosLimitsConfig.class)
@JsonSerialize(as = ImmutableQosLimitsConfig.class)
public abstract class QosLimitsConfig {

    public static final QosLimitsConfig DEFAULT_NO_LIMITS = ImmutableQosLimitsConfig.builder().build();

    @Value.Default
    public long readBytesPerSecond() {
        return Long.MAX_VALUE;
    }

    @Value.Default
    public long writeBytesPerSecond() {
        return Long.MAX_VALUE;
    }

}
