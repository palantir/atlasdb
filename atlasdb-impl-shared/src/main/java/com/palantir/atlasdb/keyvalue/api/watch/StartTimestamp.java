/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.api.watch;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Comparator;
import org.immutables.value.Value;

/**
 * Encapsulates a start timestamp to avoid the need to use excessive numbers of longs everywhere. This is only
 * intended to be used internally.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableStartTimestamp.class)
@JsonDeserialize(as = ImmutableStartTimestamp.class)
public interface StartTimestamp extends Comparable<StartTimestamp> {
    @JsonProperty("start-ts")
    @Value.Parameter
    long value();

    static StartTimestamp of(long value) {
        return ImmutableStartTimestamp.of(value);
    }

    @Override
    default int compareTo(StartTimestamp other) {
        return Comparator.comparingLong(StartTimestamp::value).compare(this, other);
    }
}
