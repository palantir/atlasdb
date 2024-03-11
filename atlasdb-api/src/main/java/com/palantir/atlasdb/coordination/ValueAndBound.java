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

package com.palantir.atlasdb.coordination;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A pair of an optionally present value and a bound indicating that the value is valid for events at timestamps up to
 * the bound, inclusive.
 *
 * In some embodiments this bound may be an AtlasDB timestamp. If a decision is to be made based on an event (such as a
 * transaction) happening at some timestamp, then this value should only be used to make the decision if it is still
 * valid at that timestamp (i.e. the bound is greater than or equal to that timestamp).
 */
@Value.Immutable
@JsonSerialize(as = ImmutableValueAndBound.class)
@JsonDeserialize(as = ImmutableValueAndBound.class)
public interface ValueAndBound<T> {
    long INVALID_BOUND = -1;

    @Value.Parameter
    Optional<T> value();

    @Value.Parameter
    long bound();

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    static <T> ValueAndBound<T> of(Optional<T> value, long bound) {
        return ImmutableValueAndBound.of(value, bound);
    }

    static <T> ValueAndBound<T> of(T value, long bound) {
        return ImmutableValueAndBound.of(Optional.of(value), bound);
    }
}
