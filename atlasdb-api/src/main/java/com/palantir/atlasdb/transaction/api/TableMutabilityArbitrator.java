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

import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Optional;
import java.util.SortedSet;

/**
 * Decides whether tables are mutable or not.
 */
public interface TableMutabilityArbitrator {
    TableMutabilityArbitrator A_PRIORI_ARBITRATOR = new TableMutabilityArbitrator() {
        @Override
        public Mutability getMutability(TableReference tableReference) {
            return Mutability.MUTABLE;
        }

        @Override
        public Optional<SortedSet<byte[]>> getExhaustiveColumnSet(TableReference tableReference) {
            return Optional.empty();
        }
    };

    Mutability getMutability(TableReference tableReference);

    // EVIL - this should not be here. I'm doing it to avoid having to wire through loads of crap again,
    // and because this thing calculates stuff with metadata.
    // Realistically this should be in its own class, kind of like what we did with TransactionKeyValueService's bits.
    // Optional empty means that the set of columns is not known.
    // The set is expected to be sorted in accordance with UnsignedBytes lexicographical comparator.
    Optional<SortedSet<byte[]>> getExhaustiveColumnSet(TableReference tableReference);
}
