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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.common.time.NanoTime;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableLeaderTime.class)
@JsonDeserialize(as = ImmutableLeaderTime.class)
public abstract class LeaderTime {
    @Value.Parameter
    public abstract LeadershipId id();

    @Value.Parameter
    public abstract NanoTime currentTime();

    public boolean isComparableWith(LeaderTime other) {
        return id().equals(other.id());
    }

    public static LeaderTime of(LeadershipId id, NanoTime time) {
        return ImmutableLeaderTime.builder()
                .id(id)
                .currentTime(time)
                .build();
    }
}
