/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

public final class HostIdEvolution {
    private HostIdEvolution() {
        // Utility class
    }

    /**
     * Returns true iff there exists a plausible sequence of cluster changes, measured through differences in snapshots
     * of the host IDs of the cluster, that could have led to the given set of snapshots.
     * <p>
     * Notice that this method may give us false negatives as the cluster may go through more than one transition in
     * between the snapshots of host IDs we are able to read. However, in the absence of UUID collisions, this method
     * will not give us false positives.
     */
    public static boolean existsPlausibleEvolutionOfHostIdSets(Set<Set<String>> sets) {
        if (sets.isEmpty()) {
            return true;
        }
        if (sets.contains(ImmutableSet.of())) {
            // If present, this will not have a nonempty intersection with any other set, so *not* all sets would be
            // connected by non-empty intersections.
            return false;
        }

        Set<Set<String>> remainingUnconnectedSets = new HashSet<>(sets);

        Iterator<Set<String>> iterator = remainingUnconnectedSets.iterator();
        Set<String> newlyVisitedElements = new HashSet<>(iterator.next());
        iterator.remove();
        boolean moreNodesToExplore = true;
        while (moreNodesToExplore) {
            Set<Set<String>> setsMatchingVisitedElements = remainingUnconnectedSets.stream()
                    .filter(hostIds ->
                            !Sets.intersection(hostIds, newlyVisitedElements).isEmpty())
                    .collect(Collectors.toSet());

            remainingUnconnectedSets.removeAll(setsMatchingVisitedElements);
            // It suffices to consider just the nodes visited on this iteration, since nodes that could have been
            // reached from the nodes visited on previous iterations would have already have been visited and removed
            // from remainingUnconnectedSets.
            newlyVisitedElements.clear();
            newlyVisitedElements.addAll(
                    setsMatchingVisitedElements.stream().flatMap(Set::stream).collect(Collectors.toSet()));
            moreNodesToExplore = !setsMatchingVisitedElements.isEmpty();
        }
        return remainingUnconnectedSets.isEmpty();
    }
}
