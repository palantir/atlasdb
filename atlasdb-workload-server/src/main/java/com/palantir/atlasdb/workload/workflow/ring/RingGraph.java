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
import one.util.streamex.EntryStream;
import one.util.streamex.IntStreamEx;

public final class RingGraph {
    private final Map<Integer, Optional<Integer>> ring;

    private RingGraph(Map<Integer, Optional<Integer>> ring) {
        this.ring = ring;
    }

    public static RingGraph init(int size) {
        return new RingGraph(IntStreamEx.range(size)
                .boxed()
                .mapToEntry(_value -> Optional.<Integer>empty())
                .toMap());
    }

    public static RingGraph from(Map<Integer, Integer> ring) {
        return new RingGraph(EntryStream.of(ring).mapValues(Optional::of).toMap());
    }

    public static RingGraph fromPartial(Map<Integer, Optional<Integer>> ring) {
        return new RingGraph(ring);
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
    public Optional<RingError> validate() {
        if (isEmpty()) {
            return Optional.empty();
        }

        Integer initialNode = ring.keySet().iterator().next();
        Set<Integer> remainingNodes = new HashSet<>(ring.keySet());
        Optional<Integer> maybeNextNode = ring.get(initialNode);
        for (int maxIterations = 0; maxIterations <= remainingNodes.size(); maxIterations++) {
            // If we do not have a next node, we are missing data, as a ring should cycle
            if (maybeNextNode.isEmpty()) {
                return Optional.of(RingError.missingEntries(ring));
            }

            Integer nextNode = maybeNextNode.get();

            // If we reference a node that does not exist in our ring, it means we are missing data
            if (!ring.containsKey(nextNode)) {
                return Optional.of(RingError.missingEntries(ring));
            }

            // If the node already has been visited, it means we're in a cycle
            if (!remainingNodes.remove(nextNode)) {
                return Optional.of(RingError.cycle(ring));
            }

            maybeNextNode = ring.get(nextNode);
        }

        return Optional.empty();
    }

    /**
     * Returns a valid new ring with the same nodes but randomly generated edges.
     */
    public RingGraph generateNewRing() {
        List<Integer> keys = ring.keySet().stream().collect(Collectors.toList());
        Collections.shuffle(keys);
        Iterator<Integer> valuesIterator = Iterators.cycle(keys);
        valuesIterator.next();
        return RingGraph.from(keys.stream()
                .sequential()
                .collect(Collectors.toMap(Function.identity(), _node -> valuesIterator.next())));
    }

    /**
     * Returns the current map of the ring.
     * @throws com.palantir.logsafe.exceptions.SafeIllegalStateException When one or more edges are missing from the ring.
     */
    public Map<Integer, Integer> asMap() {
        Preconditions.checkState(
                !anyEmpty(), "Cannot convert ring to map as some edges are missing", SafeArg.of("ring", ring));
        return EntryStream.of(ring).mapValues(Optional::get).toMap();
    }

    private boolean anyEmpty() {
        return ring.values().stream().anyMatch(Optional::isEmpty);
    }

    private boolean isEmpty() {
        return ring.values().stream().allMatch(Optional::isEmpty);
    }
}
