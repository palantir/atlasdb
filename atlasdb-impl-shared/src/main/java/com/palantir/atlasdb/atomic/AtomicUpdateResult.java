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
package com.palantir.atlasdb.atomic;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.logsafe.Unsafe;
import java.util.Collection;
import org.immutables.value.Value;

@Unsafe
@Value.Immutable
public interface AtomicUpdateResult {
    ImmutableList<Cell> knownSuccessfullyCommittedKeys();

    ImmutableList<Cell> existingKeys();

    static AtomicUpdateResult create(Collection<Cell> committed, Collection<Cell> existing) {
        return ImmutableAtomicUpdateResult.builder()
                .addAllKnownSuccessfullyCommittedKeys(committed)
                .existingKeys(existing)
                .build();
    }

    static AtomicUpdateResult success(Collection<Cell> committed) {
        return ImmutableAtomicUpdateResult.builder()
                .addAllKnownSuccessfullyCommittedKeys(committed)
                .build();
    }

    static AtomicUpdateResult success(Cell committed) {
        return ImmutableAtomicUpdateResult.builder()
                .addKnownSuccessfullyCommittedKeys(committed)
                .build();
    }

    static AtomicUpdateResult keyAlreadyExists(Collection<Cell> existingKeys) {
        return ImmutableAtomicUpdateResult.builder()
                .addAllExistingKeys(existingKeys)
                .build();
    }

    static AtomicUpdateResult keyAlreadyExists(Cell existingKey) {
        return ImmutableAtomicUpdateResult.builder()
                .addExistingKeys(existingKey)
                .build();
    }
}
