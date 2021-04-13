/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.SortedSetMultimap;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableTimestampStateStoreState.class)
@JsonDeserialize(as = ImmutableTimestampStateStoreState.class)
interface TimestampStateStoreState {
    Map<StartTimestamp, TimestampStateStore.MapEntry> timestampMap();

    SortedSetMultimap<Sequence, StartTimestamp> livingVersions();
}
