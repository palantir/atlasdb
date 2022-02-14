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
import org.immutables.value.Value;

@Value.Modifiable
interface UpdateFailureRecord {
    int failureCount();

    Set<String> lockIds();

    static ModifiableUpdateFailureRecord of(String lockId) {
        return ModifiableUpdateFailureRecord.create().setFailureCount(1).addLockIds(lockId);
    }

    static ModifiableUpdateFailureRecord merge(
            ModifiableUpdateFailureRecord existingRecord, ModifiableUpdateFailureRecord newRecord) {
        return existingRecord
                .setFailureCount(existingRecord.failureCount() + newRecord.failureCount())
                .addAllLockIds(newRecord.lockIds());
    }

    @Value.Derived
    default boolean isConsistent(int expectedResponseCount) {
        return failureCount() == expectedResponseCount && lockIds().size() == 1;
    }
}
