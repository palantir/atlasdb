/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.persistentlock;


import java.util.Optional;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonSerialize(as = ImmutablePersistentLockServiceResponse.class)
@JsonDeserialize(as = ImmutablePersistentLockServiceResponse.class)
public interface PersistentLockServiceResponse {
    Optional<LockEntry> lockEntry();

    boolean isSuccessful();

    @Value.Default
    default String message() {
        return "";
    }

    static PersistentLockServiceResponse successfulResponseWithLockEntry(LockEntry lockEntry) {
        return PersistentLockServiceResponse.builder()
                .lockEntry(lockEntry)
                .isSuccessful(true)
                .build();
    }

    static PersistentLockServiceResponse failureResponseWithMessage(String message) {
        return PersistentLockServiceResponse.builder()
                .isSuccessful(false)
                .message(message)
                .build();
    }

    static PersistentLockServiceResponse successfulResponseWithMessage(String message) {
        return PersistentLockServiceResponse.builder()
                .isSuccessful(true)
                .message(message)
                .build();
    }

    class Builder extends ImmutablePersistentLockServiceResponse.Builder {}

    static Builder builder() {
        return new Builder();
    }
}
