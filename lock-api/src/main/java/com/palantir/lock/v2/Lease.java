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

import java.time.Duration;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.common.time.NanoTime;

@Value.Immutable
@JsonSerialize(as = ImmutableLease.class)
@JsonDeserialize(as = ImmutableLease.class)
public abstract class Lease {
    @Value.Parameter
    public abstract IdentifiedTime leaseOwnerId();

    @Value.Parameter
    public abstract NanoTime startTime();

    @Value.Parameter
    public abstract Duration validity();

    public boolean isValid(IdentifiedTime identifiedTime) {
        return leaseOwnerId().equals(identifiedTime.leadershipId())
                && identifiedTime.currentTimeNanos().isBefore(expiry());
    }

    public NanoTime expiry() {
        return startTime().plus(validity());
    }

    public static Lease of(IdentifiedTime identifiedTime, Duration validity) {
        return ImmutableLease.of(
                identifiedTime.leadershipId(),
                identifiedTime.currentTimeNanos(),
                validity);
    }

    public static Lease of(LeadershipId leadershipId, NanoTime startTime, Duration period) {
        return Lease.of(IdentifiedTime.of(leadershipId, startTime), period);
    }
}
