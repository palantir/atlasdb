/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.transaction.client;

import com.google.common.base.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.immutables.value.Value;

/**
 * Distributes residues of a given modulus in a balanced fashion (though we don't automatically rebalance between
 * residues).
 *
 * The implementation of this class takes time linear in the number of residues for getAndMarkResidue and unmarkResidue.
 * A logarithmic implementation with balanced trees is possible, but we haven't implemented it as in our usage the
 * number of residues is very small.
 */
public class DistributingModulusGenerator {
    private final SortedSet<ReferenceCountedResidue> referenceCounts;

    public DistributingModulusGenerator(int modulus) {
        com.palantir.logsafe.Preconditions.checkArgument(modulus > 0, "Modulus must be positive");
        this.referenceCounts = IntStream.range(0, modulus)
                .mapToObj(value -> ImmutableReferenceCountedResidue.of(0, value))
                .collect(Collectors.toCollection(() -> new TreeSet<>(ReferenceCountedResidue.RESIDUE_COMPARATOR)));
    }

    public synchronized int getAndMarkResidue() {
        ReferenceCountedResidue chosenResidue = getRandomLeastReferencedReferenceCountingResidue();
        referenceCounts.remove(chosenResidue);
        referenceCounts.add(chosenResidue.mark());
        return chosenResidue.residue();
    }

    public synchronized void unmarkResidue(int residue) {
        // There are usually only 16 elements, so this O(n) algo probably will do, but we can pair this with a HashMap
        // and/or make ReferenceCountedResidue modifiable if we decide to use higher moduli in the future.
        for (ReferenceCountedResidue referenceCountedResidue : referenceCounts) {
            if (referenceCountedResidue.residue() == residue) {
                Preconditions.checkState(
                        referenceCountedResidue.references() > 0,
                        "Attempted to unmark residue %s when it had no references",
                        referenceCountedResidue.residue());
                referenceCounts.remove(referenceCountedResidue);
                referenceCounts.add(referenceCountedResidue.unmark());
                return;
            }
        }
        throw new IllegalStateException("Residue " + residue + " was not found when unmarking!");
    }

    private ReferenceCountedResidue getRandomLeastReferencedReferenceCountingResidue() {
        SortedSet<ReferenceCountedResidue> leastReferencedResidues = getAllLeastReferencedResidues();
        int elementIndex = ThreadLocalRandom.current().nextInt(leastReferencedResidues.size());
        return leastReferencedResidues.stream()
                .skip(elementIndex)
                .findFirst()
                .orElseThrow(() -> new SafeIllegalStateException(
                        "Attempted to select a random element from an empty collection!"));
    }

    private SortedSet<ReferenceCountedResidue> getAllLeastReferencedResidues() {
        return referenceCounts.headSet(upperBoundOnEquallyReferencedResidues(referenceCounts.first()));
    }

    private static ReferenceCountedResidue upperBoundOnEquallyReferencedResidues(ReferenceCountedResidue residue) {
        return ImmutableReferenceCountedResidue.of(residue.references() + 1, Integer.MIN_VALUE);
    }

    @Value.Immutable
    interface ReferenceCountedResidue {
        Comparator<ReferenceCountedResidue> RESIDUE_COMPARATOR = Comparator.comparing(
                        ReferenceCountedResidue::references)
                .thenComparing(ReferenceCountedResidue::residue);

        @Value.Parameter
        int references();

        @Value.Parameter
        int residue();

        @Value.Check
        default void check() {
            com.palantir.logsafe.Preconditions.checkState(references() >= 0, "Reference counts must not be negative");
        }

        default ReferenceCountedResidue mark() {
            return ImmutableReferenceCountedResidue.builder()
                    .from(this)
                    .references(references() + 1)
                    .build();
        }

        default ReferenceCountedResidue unmark() {
            return ImmutableReferenceCountedResidue.builder()
                    .from(this)
                    .references(references() - 1)
                    .build();
        }
    }
}
