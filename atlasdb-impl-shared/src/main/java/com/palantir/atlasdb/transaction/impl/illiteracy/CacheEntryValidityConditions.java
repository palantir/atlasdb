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

package com.palantir.atlasdb.transaction.impl.illiteracy;

import java.util.Objects;

import org.immutables.value.Value;

@Value.Immutable
public interface CacheEntryValidityConditions {
    WatchIdentifierAndState watchIdentifierAndState();
    long firstTimestampAtWhichReadIsValid();

    default CacheValidity evaluate(WatchIdentifierAndState stateAtRead, long startTimestamp) {
        if (stateAtRead.indexState().lockIsHeld()) {
            // This is more "we don't need to update yet, it's pointless"
            return CacheValidity.VALID_BUT_NOT_USABLE_HERE;
        }

        if (!Objects.equals(stateAtRead.identifier(), watchIdentifierAndState().identifier())) {
            // The watch identifier has changed. Possible leader election.
            // Accept a bit of thrashing at election time, as long as it resolves itself quickly.
            return CacheValidity.NO_LONGER_VALID;
        }

        if (stateAtRead.indexState().compareTo(watchIdentifierAndState().indexState()) > 0) {
            // We are reading and the lock has moved on AND is not held, so we should update the cache.
            return CacheValidity.NO_LONGER_VALID;
        }

        if (startTimestamp < firstTimestampAtWhichReadIsValid()) {
            // Cache is valid but our read is a bit early (maybe we are a long running transaction)
            return CacheValidity.VALID_BUT_NOT_USABLE_HERE;
        }

        return CacheValidity.USABLE;
    }
}
