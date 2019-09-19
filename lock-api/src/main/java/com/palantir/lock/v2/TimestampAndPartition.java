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

package com.palantir.lock.v2;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * A pair of a timestamp along with a numeric partition identifier that may be used by some Atlas clients operating
 * some key-value services to more efficiently store data.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableTimestampAndPartition.class)
@JsonDeserialize(as = ImmutableTimestampAndPartition.class)
public interface TimestampAndPartition {
    @Value.Parameter
    long timestamp();

    /**
     * A numeric partition identifier. Some Atlas clients may use this identifier to more efficiently store data.
     * The presence of a partition in general does not guarantee that an Atlas client will use it in any way.
     */
    @Value.Parameter
    int partition();

    static TimestampAndPartition of(long timestamp, int partition) {
        return ImmutableTimestampAndPartition.of(timestamp, partition);
    }
}
