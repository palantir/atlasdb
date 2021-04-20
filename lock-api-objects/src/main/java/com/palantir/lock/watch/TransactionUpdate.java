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

package com.palantir.lock.watch;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.lock.v2.LockToken;
import org.immutables.value.Value;

/**
 * This is a record of a transaction that successfully retrieved a commit timestamp, along with the lock token it
 * acquired for its writes. Users MUST NOT assume that this transaction has successfully committed - it may or may not
 * have.
 */
@Value.Immutable
@JsonSerialize(as = ImmutableTransactionUpdate.class)
@JsonDeserialize(as = ImmutableTransactionUpdate.class)
public interface TransactionUpdate {
    long startTs();

    long commitTs();

    LockToken writesToken();

    static ImmutableTransactionUpdate.Builder builder() {
        return ImmutableTransactionUpdate.builder();
    }
}
