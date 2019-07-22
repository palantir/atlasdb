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

package com.palantir.atlasdb.timelock.watch;

import java.util.UUID;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(as = ImmutableWatchIdentifier.class)
@JsonDeserialize(as = ImmutableWatchIdentifier.class)
@Value.Immutable
public interface WatchIdentifier {
    @JsonValue
    String identifier();

    static WatchIdentifier of(UUID uuid) {
        return ImmutableWatchIdentifier.builder().identifier(uuid.toString()).build();
    }

    @JsonCreator
    static WatchIdentifier of(String id) {
        return ImmutableWatchIdentifier.builder().identifier(id).build();
    }
}
