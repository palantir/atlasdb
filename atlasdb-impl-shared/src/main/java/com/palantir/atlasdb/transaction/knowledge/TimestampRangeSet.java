/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.knowledge;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.TreeRangeSet;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableTimestampRangeSet.class)
@JsonDeserialize(as = ImmutableTimestampRangeSet.class)
public interface TimestampRangeSet {
    /**
     * Ranges of timestamps included in this set. Depending on the concrete type {@link TreeRangeSet} is not ideal,
     * but is required to ensure that ranges are serialized in a deterministic way (because this happens by iterating
     * through the map), and there does not exist a generic interface for a sorted range-set.
     */
    @Value.Parameter
    TreeRangeSet<Long> timestampRanges();
}
