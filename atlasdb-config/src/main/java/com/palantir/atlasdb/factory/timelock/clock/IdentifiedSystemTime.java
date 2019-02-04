/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.factory.timelock.clock;

import java.util.UUID;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonSerialize(as = ImmutableIdentifiedSystemTime.class)
@JsonDeserialize(as = ImmutableIdentifiedSystemTime.class)
public interface IdentifiedSystemTime {
    @Value.Parameter
    long getTimeNanos();

    /**
     * Returns a unique identifier per JVM launch, which enables detecting JVM restarts,
     * where the clock offset may change.
     */
    @Value.Parameter
    UUID getSystemId();

    static IdentifiedSystemTime of(long time, UUID systemId) {
        return ImmutableIdentifiedSystemTime.of(time, systemId);
    }
}
