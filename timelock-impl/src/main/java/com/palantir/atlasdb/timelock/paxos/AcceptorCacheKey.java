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

package com.palantir.atlasdb.timelock.paxos;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.UUID;
import org.immutables.value.Value;

/**
 * This represents a snapshot of the {@link AcceptorCache}. That is, if you were to receive a
 * {@link AcceptorCacheDigest} with this {@link AcceptorCacheKey} and then you were to make a subsequent request with
 * this particular cache key, you will either receive an empty response or updates that occurred to the cache after the
 * point in time that is associated with this cache key.
 */
@Value.Immutable
@JsonDeserialize(as = ImmutableAcceptorCacheKey.class)
@JsonSerialize(as = ImmutableAcceptorCacheKey.class)
public abstract class AcceptorCacheKey {

    @JsonValue
    @Value.Parameter
    public abstract UUID value();

    public static AcceptorCacheKey valueOf(String key) {
        return ImmutableAcceptorCacheKey.of(UUID.fromString(key));
    }

    public static AcceptorCacheKey newCacheKey() {
        return ImmutableAcceptorCacheKey.of(UUID.randomUUID());
    }

    @Override
    public String toString() {
        return value().toString();
    }
}
