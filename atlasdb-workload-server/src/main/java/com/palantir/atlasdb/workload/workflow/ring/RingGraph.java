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

package com.palantir.atlasdb.workload.workflow.ring;

import com.google.common.collect.Iterators;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import one.util.streamex.EntryStream;

public final class RingGraph {
    private final Map<Integer, Integer> ring;

    private RingGraph(Map<Integer, Integer> ring) {
        this.ring = ring;
    }

    public static RingGraph from(Map<Integer, Integer> ring) throws RingValidationException {
        validateGraphIsRing(ring);
        return new RingGraph(ring);
    }

    public static RingGraph fromPartial(Map<Integer, Optional<Integer>> ring) throws RingValidationException {
        if (isEmpty(ring)) {
            return RingGraph.generateNewRing(ring.keySet());
        }

        Preconditions.checkArgument(
                !anyEmpty(ring),
                "Graph contains missing entries, thus cannot be made into a ring.",
                SafeArg.of("ring", ring));

        return from(EntryStream.of(ring).mapValues(Optional::get).toMap());
    }

    public static RingGraph create(int size) {
        return generateNewRing(IntStream.range(0, size).boxed().collect(Collectors.toSet()));
    }

    /**
     * Checks that the ring is correct by verifying:
     * (1) There are no missing entries
     * (2) There are no sub-cycles
     * (3) The ring is connected
     *
     * This is simply done by iterating over the length of the ring, and validating that we visited each node.
     * To ensure that the ring is connected, we do not track our entry into the ring (i.e. the first node we visit),
     * as we expect to have it be visited again as the last node.
     */
    public static void validateGraphIsRing(Map<Integer, Integer> ring) throws RingValidationException {
        Integer initialNode = ring.keySet().iterator().next();
        Set<Integer> remainingNodes = new HashSet<>(ring.keySet());
        int maxIterations = ring.keySet().size();
        Integer nextNode = ring.get(initialNode);
        for (int iterations = 0; iterations < maxIterations; iterations++) {
            // If we reference a node that does not exist in our ring, it means we are missing data
            if (!ring.containsKey(nextNode)) {
                RingValidationException.throwMissingEntries(ring);
            }

            // If the node already has been visited, it means we're in a cycle within our ring
            if (!remainingNodes.remove(nextNode)) {
                RingValidationException.throwCycle(ring);
            }

            nextNode = ring.get(nextNode);
        }
    }

    public RingGraph generateNewRing() {
        return generateNewRing(ring.keySet());
    }

    /**
     * Returns a valid new ring with the same nodes but randomly generated edges.
     */
    private static RingGraph generateNewRing(Set<Integer> nodes) {
        List<Integer> keys = nodes.stream().collect(Collectors.toList());
        Collections.shuffle(keys);
        Iterator<Integer> valuesIterator = Iterators.cycle(keys);
        valuesIterator.next();
        return new RingGraph(keys.stream()
                .sequential()
                .collect(Collectors.toMap(Function.identity(), _node -> valuesIterator.next())));
    }

    public Map<Integer, Integer> asMap() {
        return ring;
    }

    private static boolean isEmpty(Map<Integer, Optional<Integer>> ring) {
        return ring.values().stream().allMatch(Optional::isEmpty);
    }

    private static boolean anyEmpty(Map<Integer, Optional<Integer>> ring) {
        return ring.values().stream().anyMatch(Optional::isEmpty);
    }
}
