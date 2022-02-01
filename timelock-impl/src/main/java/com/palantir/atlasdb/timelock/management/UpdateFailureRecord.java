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

package com.palantir.atlasdb.timelock.management;

import java.util.Set;
import java.util.UUID;
import org.immutables.value.Value;

@Value.Immutable
interface UpdateFailureRecord {
    int failureCount();

    Set<UUID> lockIds();

    static UpdateFailureRecord of(UUID lockId) {
        return ImmutableUpdateFailureRecord.builder()
                .failureCount(1)
                .addLockIds(lockId)
                .build();
    }

    static UpdateFailureRecord merge(UpdateFailureRecord existingRecord, UpdateFailureRecord newRecord) {
        return ImmutableUpdateFailureRecord.builder()
                .failureCount(existingRecord.failureCount() + newRecord.failureCount())
                .addAllLockIds(existingRecord.lockIds())
                .addAllLockIds(newRecord.lockIds())
                .build();
    }
}
