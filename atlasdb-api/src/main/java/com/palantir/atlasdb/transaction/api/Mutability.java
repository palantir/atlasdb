/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.api;

/**
 * Describes how the write pattern of an AtlasDB table looks like. AtlasDB may use this information to perform
 * protocol optimisations. AtlasDB will try to limit the extent to which users violate these settings, but is not able
 * to do this in all cases, so users that specify these incorrectly may expose themselves to logical corruption
 * (along similar lines of users declaring an incorrect conflict handler).
 * <p>
 * Mutability for a given table can be changed over time, though this must be done with caution. Downgrading
 * immutability is generally simple, though requires a schema check-point as we first turn off the optimisations, make
 * sure that no one is using them anymore, and then relax the actual behavioural constraints (permitting deletes
 * or overwrites). Upgrading immutability is more complex because some of the optimisations affect processes like sweep.
 * In theory, you need to stop all transactions that might perform deletes, and then get a fresh timestamp. Then, you
 * need to make sure sweep progress goes past this timestamp. Only after that can we be certain sweep has processed
 * everything without optimisations, and can we switch to strong immutable.
 */
public enum Mutability {
    // This is the "standard" setting: cells in tables can be written to freely.
    MUTABLE,
    // Cells are written to at most twice: they are written, and then deleted strictly after they are written
    // (with blind deletes not allowed).
    WEAK_IMMUTABLE,
    // Cells are written to once and only once, and are never deleted.
    STRONG_IMMUTABLE;

    public boolean isAtLeastWeakImmutable() {
        return this == Mutability.WEAK_IMMUTABLE || this == Mutability.STRONG_IMMUTABLE;
    }

    public boolean isAtLeastStrongImmutable() {
        return this == Mutability.STRONG_IMMUTABLE;
    }
}
