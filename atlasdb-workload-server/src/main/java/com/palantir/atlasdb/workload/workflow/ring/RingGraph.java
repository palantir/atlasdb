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

    public Optional<RingError> validate() {
        if (isEmpty()) {
            return Optional.empty();
        }

        return checkForCycleAndMissingEntries();
    }

    public Map<Integer, Integer> createOrShuffle() {
        List<Integer> keys = ring.keySet().stream().collect(Collectors.toList());
        Collections.shuffle(keys);
        Iterator<Integer> valuesIterator = Iterators.cycle(keys);
        valuesIterator.next();
        return keys.stream()
                .sequential()
                .collect(Collectors.toMap(Function.identity(), _node -> valuesIterator.next()));
    }

    /**
     * Checks that the ring is correct by ensuring we visit every node, including the root, twice.
     */
    private Optional<RingError> checkForCycleAndMissingEntries() {
        Integer initialNode = ring.keySet().iterator().next();
        Integer maxIterations = ring.size() - 1;
        Set<Integer> remainingNodes = new HashSet<>(ring.keySet());
        Optional<Integer> maybeNextNode = ring.get(initialNode);
        while (maxIterations >= 0) {
            // If we reference a node that does not exist, it means we are missing data
            if (maybeNextNode.isEmpty()) {
                return Optional.of(RingError.missingEntries(ring));
            }

            Integer nextNode = maybeNextNode.get();

            // If we reference a node that does not exist, it means we are missing data
            if (!ring.containsKey(nextNode)) {
                return Optional.of(RingError.missingEntries(ring));
            }

            // If the node already has been visited, it means we're in a cycle
            if (!remainingNodes.remove(nextNode)) {
                return Optional.of(RingError.cycle(ring));
            }

            maybeNextNode = ring.get(nextNode);
            maxIterations--;
        }

        return Optional.empty();
    }

    private boolean isEmpty() {
        return ring.values().stream().allMatch(Optional::isEmpty);
    }
}
