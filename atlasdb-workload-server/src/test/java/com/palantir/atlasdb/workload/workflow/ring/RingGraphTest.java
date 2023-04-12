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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class RingGraphTest {

    private static final Map<Integer, Integer> VALID_RING = Map.of(0, 2, 1, 0, 2, 1);

    @Test
    public void validateReturnsOptionalWhenEntireRingEmpty() {
        assertThat(RingGraph.fromPartial(Map.of()).validate()).isEmpty();
    }

    @Test
    public void validateReturnsMissingEntriesErrorWhenSomeValuesEmpty() {
        Map<Integer, Optional<Integer>> partiallyEmptyRing = Map.of(0, Optional.of(1), 1, Optional.empty());
        assertThat(RingGraph.fromPartial(partiallyEmptyRing).validate())
                .hasValue(RingError.missingEntries(partiallyEmptyRing));
    }

    @Test
    public void validateReturnsMissingEntriesErrorWhenNonExistentNodesReferenced() {
        Map<Integer, Optional<Integer>> missingNodesRing = Map.of(0, Optional.of(2), 1, Optional.of(0));
        assertThat(RingGraph.fromPartial(missingNodesRing).validate())
                .hasValue(RingError.missingEntries(missingNodesRing));
    }

    @Test
    public void validateReturnsCycleErrorWhenUnableToReachEveryNode() {
        Map<Integer, Optional<Integer>> incompleteRing =
                Map.of(0, Optional.of(1), 1, Optional.of(1), 2, Optional.of(0));
        assertThat(RingGraph.fromPartial(incompleteRing).validate()).hasValue(RingError.cycle(incompleteRing));
    }

    @Test
    public void validateReturnsCycleErrorWhenRootNodeNotRevisited() {
        Map<Integer, Optional<Integer>> incompleteRing = Map.of(0, Optional.of(1), 1, Optional.of(1));
        assertThat(RingGraph.fromPartial(incompleteRing).validate()).hasValue(RingError.cycle(incompleteRing));
    }

    @Test
    public void validateReturnsEmptyForValidRing() {
        assertThat(RingGraph.from(VALID_RING).validate()).isEmpty();
    }

    @Test
    public void generateNewRingReturnsValidRing() {
        RingGraph ring = RingGraph.init(8);
        for (int count = 0; count < 1000; count++) {
            ring = ring.generateNewRing();
            assertThat(ring.validate()).isEmpty();
        }
    }

    @Test
    public void generateNewRingChangesRing() {
        RingGraph ring = RingGraph.from(Map.of(0, 1, 1, 1));
        assertThat(ring.validate()).isNotEmpty();
        assertThat(ring.generateNewRing().validate()).isEmpty();
    }

    @Test
    public void asMapThrowsWhenMissingEdge() {
        Map<Integer, Optional<Integer>> missingEdge = Map.of(0, Optional.of(1), 1, Optional.empty());
        assertThatThrownBy(() -> RingGraph.fromPartial(missingEdge).asMap())
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessageContaining("Cannot generate ring to map as some edges are missing");
    }

    @Test
    public void asMapConvertsCorrectly() {
        assertThat(RingGraph.from(VALID_RING).asMap()).isEqualTo(VALID_RING);
    }
}
