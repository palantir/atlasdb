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

@Value.Immutable
@JsonSerialize(as = ImmutableStartTransactionResponseV4.class)
@JsonDeserialize(as = ImmutableStartTransactionResponseV4.class)
public interface StartTransactionResponseV4 {
    @Value.Parameter
    LockImmutableTimestampResponse immutableTimestamp();

    @Value.Parameter
    PartitionedTimestamps timestamps();

    @Value.Parameter
    Lease lease();

    static StartTransactionResponseV4 of(
            LockImmutableTimestampResponse immutableTimestamp,
            PartitionedTimestamps startTimestamp,
            Lease lease) {
        return ImmutableStartTransactionResponseV4.of(immutableTimestamp, startTimestamp, lease);
    }
}
