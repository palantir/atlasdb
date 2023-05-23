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

package com.palantir.atlasdb.workload.invariant;

import java.util.Optional;
import org.immutables.value.Value;

/**
 * Occasionally, storing just a value might not be enough context needed when validating an invariant.
 *
 * For example, to detect that the ABA problem has occurred (ABA problem (https://en.wikipedia.org/wiki/ABA_problem),
 * values alone would not be sufficient. Storing the timestamp associated with the value, such as the start timestamp,
 * would be able to catch these sort of cases.
 */
@Value.Immutable
public interface ValueAndTimestamp {
    ValueAndTimestamp EMPTY = of(Optional.empty(), Optional.empty());

    @Value.Parameter
    Optional<Integer> value();

    @Value.Parameter
    Optional<Long> timestamp();

    static ValueAndTimestamp empty() {
        return EMPTY;
    }

    static ValueAndTimestamp of(Optional<Integer> value) {
        return of(value, Optional.empty());
    }

    static ValueAndTimestamp of(Optional<Integer> value, Long timestamp) {
        return of(value, Optional.of(timestamp));
    }

    static ValueAndTimestamp of(Integer value, Long timestamp) {
        return of(Optional.of(value), Optional.of(timestamp));
    }

    static ValueAndTimestamp of(Optional<Integer> value, Optional<Long> timestamp) {
        return ImmutableValueAndTimestamp.of(value, timestamp);
    }
}
