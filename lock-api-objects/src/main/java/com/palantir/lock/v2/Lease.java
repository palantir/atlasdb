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
import java.time.Duration;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableLease.class)
@JsonDeserialize(as = ImmutableLease.class)
public abstract class Lease {
    @Value.Parameter
    public abstract LeaderTime leaderTime();

    @Value.Parameter
    abstract Duration validity();

    public boolean isValid(LeaderTime currentLeaderTime) {
        return leaderTime().isComparableWith(currentLeaderTime)
                && currentLeaderTime.currentTime().isBefore(expiry());
    }

    public NanoTime expiry() {
        return leaderTime().currentTime().plus(validity());
    }

    public static Lease of(LeaderTime leaderTime, Duration validity) {
        return ImmutableLease.builder()
                .leaderTime(leaderTime)
                .validity(validity)
                .build();
    }
}
