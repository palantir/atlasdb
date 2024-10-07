/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableTimestampRange.class)
@JsonDeserialize(as = ImmutableTimestampRange.class)
public interface TimestampRange {
    @Value.Parameter
    long startInclusive();

    @Value.Parameter
    long endExclusive();

    static TimestampRange of(long startInclusive, long endExclusive) {
        return ImmutableTimestampRange.of(startInclusive, endExclusive);
    }

    static TimestampRange openBucket(long startInclusive) {
        // Think very carefully about changing from -1 without a migration
        return of(startInclusive, -1);
    }
}
