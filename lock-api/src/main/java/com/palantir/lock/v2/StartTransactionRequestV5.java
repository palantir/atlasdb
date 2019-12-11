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

package com.palantir.lock.v2;

import java.util.OptionalLong;
import java.util.UUID;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable
@JsonSerialize(as = ImmutableStartTransactionRequestV5.class)
@JsonDeserialize(as = ImmutableStartTransactionRequestV5.class)
public interface StartTransactionRequestV5 {
    UUID requestId();
    UUID requestorId();
    OptionalLong lastKnownLockLogVersion();
    int numTransactions();

    static StartTransactionRequestV5 createForRequestor(UUID requestorId, OptionalLong lockVersion, int number) {
        return ImmutableStartTransactionRequestV5.builder()
                .requestId(UUID.randomUUID())
                .requestorId(requestorId)
                .lastKnownLockLogVersion(lockVersion)
                .numTransactions(number)
                .build();
    }
}
