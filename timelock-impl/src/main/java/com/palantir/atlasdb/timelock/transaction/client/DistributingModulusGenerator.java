/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.transaction.client;

import java.util.Comparator;
import java.util.SortedSet;
import java.util.stream.IntStream;

import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Distributes residues of a given modulus in a balanced fashion (though we don't automatically rebalance between
 * residues).
 *
 * Unmarking residues may not be performant if the number of residues used is large.
 */
public class DistributingModulusGenerator {
    private final SortedSet<ReferenceCountedResidue> referenceCounts;

    @VisibleForTesting
    DistributingModulusGenerator(SortedSet<ReferenceCountedResidue> referenceCounts) {
        this.referenceCounts = referenceCounts;
    }

    public static DistributingModulusGenerator create(int modulus) {
        SortedSet<ReferenceCountedResidue> referenceCounts
                = Sets.newTreeSet(
                        Comparator.comparing(ReferenceCountedResidue::references)
                                .thenComparing(ReferenceCountedResidue::residue));
        IntStream.range(0, modulus)
                .forEach(value -> referenceCounts.add(ImmutableReferenceCountedResidue.of(0, value)));
        return new DistributingModulusGenerator(referenceCounts);
    }

    public synchronized int getAndMarkResidue() {
        ReferenceCountedResidue leastReferenced = referenceCounts.first();
        referenceCounts.remove(leastReferenced);
        referenceCounts.add(
                ImmutableReferenceCountedResidue.of(leastReferenced.references() + 1, leastReferenced.residue()));
        return leastReferenced.residue();
    }

    public synchronized void unmarkResidue(int residue) {
        // There are usually only 16 elements, so this O(n) algo probably will do, but we can pair this with a HashMap
        // if we decide we need more performance.
        for (ReferenceCountedResidue referenceCountedResidue : referenceCounts) {
            if (referenceCountedResidue.residue() == residue) {
                Preconditions.checkState(
                        referenceCountedResidue.references() > 0,
                        "Attempted to unmark residue %s when it had no references",
                        referenceCountedResidue.residue());
                referenceCounts.remove(referenceCountedResidue);
                referenceCounts.add(
                        ImmutableReferenceCountedResidue.of(
                                referenceCountedResidue.references() - 1, referenceCountedResidue.residue()));
                return;
            }
        }
    }

    @Value.Immutable
    interface ReferenceCountedResidue {
        @Value.Parameter
        int references();

        @Value.Parameter
        int residue();
    }
}
