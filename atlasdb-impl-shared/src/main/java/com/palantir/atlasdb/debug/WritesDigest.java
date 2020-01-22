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

package com.palantir.atlasdb.debug;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.SortedMap;
import java.util.SortedSet;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableWritesDigest.class)
@JsonSerialize(as = ImmutableWritesDigest.class)
@Value.Immutable
public interface WritesDigest<T> {
    @Value.NaturalOrder
    SortedSet<Long> allWrittenTimestamps();

    @Value.NaturalOrder
    SortedMap<Long, Long> completedOrAbortedTransactions();

    @Value.NaturalOrder
    SortedSet<Long> inProgressTransactions();

    @Value.NaturalOrder
    SortedMap<Long, T> allWrittenValuesDeserialized();
}
