/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.lock.v2;

import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonSerialize(as = ImmutableLockResponse.class)
@JsonDeserialize(as = ImmutableLockResponse.class)
public interface LockResponse {

    @Value.Parameter
    Optional<LockToken> getTokenOrEmpty();

    @JsonIgnore
    default boolean wasSuccessful() {
        return getTokenOrEmpty().isPresent();
    }

    @JsonIgnore
    default LockToken getToken() {
        if (!wasSuccessful()) {
            throw new IllegalStateException("This lock response was not successful");
        }
        return getTokenOrEmpty().get();
    }

    static LockResponse successful(LockToken token) {
        return ImmutableLockResponse.of(Optional.of(token));
    }

    static LockResponse timedOut() {
        return ImmutableLockResponse.of(Optional.empty());
    }

}
