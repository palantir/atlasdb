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

import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.common.collect.ImmutableMap;
import com.palantir.logsafe.SafeArg;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public final class RingGraphTest {

    private static final Map<Integer, Integer> VALID_RING = ImmutableMap.of(0, 2, 1, 0, 2, 1);

    @Test
    public void fromPartialGeneratesRandomRingWhenAllKeysEmpty() {
        Map<Integer, Optional<Integer>> emptyEdges = ImmutableMap.of(1, Optional.empty(), 2, Optional.empty());
        assertThatCode(() -> RingGraph.fromPartial(emptyEdges)).doesNotThrowAnyException();
    }

    @Test
    public void fromPartialThrowsWhenSomeEntriesAreEmpty() {
        Map<Integer, Optional<Integer>> partiallyEmptyRing = ImmutableMap.of(0, Optional.of(1), 1, Optional.empty());
        assertThatLoggableExceptionThrownBy(() -> RingGraph.fromPartial(partiallyEmptyRing))
                .isInstanceOf(RingValidationException.class)
                .hasExactlyArgs(
                        SafeArg.of("type", RingValidationException.Type.INCOMPLETE_RING),
                        SafeArg.of("ring", partiallyEmptyRing));
    }

    @Test
    public void fromThrowsMissingEntriesWhenNodeReferencedDoesNotExist() {
        Map<Integer, Optional<Integer>> missingNodesRing = ImmutableMap.of(0, Optional.of(2), 1, Optional.of(0));
        assertThatLoggableExceptionThrownBy(() -> RingGraph.fromPartial(missingNodesRing))
                .isInstanceOf(RingValidationException.class)
                .hasExactlyArgs(
                        SafeArg.of("type", RingValidationException.Type.MISSING_REFERENCE),
                        SafeArg.of("ring", missingNodesRing));
    }

    @Test
    public void fromThrowsCycleExceptionWhenUnableToReachEveryNode() {
        Map<Integer, Optional<Integer>> incompleteRing =
                ImmutableMap.of(0, Optional.of(1), 1, Optional.of(1), 2, Optional.of(0));
        assertThatLoggableExceptionThrownBy(() -> RingGraph.fromPartial(incompleteRing))
                .isInstanceOf(RingValidationException.class)
                .hasExactlyArgs(
                        SafeArg.of("type", RingValidationException.Type.EARLY_CYCLE),
                        SafeArg.of("ring", incompleteRing));
    }

    @Test
    public void fromThrowsCycleExceptionWhenEntryNodeNotRevisited() {
        Map<Integer, Optional<Integer>> incompleteRing = ImmutableMap.of(0, Optional.of(1), 1, Optional.of(1));
        assertThatLoggableExceptionThrownBy(() -> RingGraph.fromPartial(incompleteRing))
                .isInstanceOf(RingValidationException.class)
                .hasExactlyArgs(
                        SafeArg.of("type", RingValidationException.Type.EARLY_CYCLE),
                        SafeArg.of("ring", incompleteRing));
    }

    @Test
    public void generateNewRingCreatesDifferentRing() {
        RingGraph ringGraph = RingGraph.create(8);
        boolean uniqueAtLeastOnce = false;
        for (int iteration = 0; iteration < 10; iteration++) {
            if (!ringGraph.generateNewRing().asMap().equals(ringGraph.asMap())) {
                uniqueAtLeastOnce = true;
                break;
            }
        }
        assertThat(uniqueAtLeastOnce)
                .as("Check ring generation multiple times as we could randomly generate an identical ring")
                .isTrue();
    }

    @Test
    public void generateNewRingReturnsValidRing() {
        RingGraph ring = RingGraph.create(8);
        for (int count = 0; count < 1000; count++) {
            assertThatCode(() -> RingGraph.from(ring.generateNewRing().asMap())).doesNotThrowAnyException();
        }
    }

    @Test
    public void asMapConvertsCorrectly() {
        assertThat(RingGraph.from(VALID_RING).asMap()).containsExactlyInAnyOrderEntriesOf(VALID_RING);
    }
}
