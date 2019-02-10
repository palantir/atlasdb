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

package com.palantir.lock.v2;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.common.time.NanoTime;

@Value.Immutable
@JsonSerialize(as = ImmutableIdentifiedTime.class)
@JsonDeserialize(as = ImmutableIdentifiedTime.class)
public interface IdentifiedTime {
    @Value.Parameter
    LeadershipId leadershipId();

    @Value.Parameter
    NanoTime currentTimeNanos();

    static IdentifiedTime of(LeadershipId id, NanoTime time) {
        return ImmutableIdentifiedTime.of(id, time);
    }
}
