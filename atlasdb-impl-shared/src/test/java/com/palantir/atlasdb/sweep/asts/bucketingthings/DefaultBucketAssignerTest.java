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

package com.palantir.atlasdb.sweep.asts.bucketingthings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.sweep.asts.TimestampRange;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketWriter.WriteState;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketAssigner.BucketAssignerMetrics;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketAssigner.IterationResult.OperationResult;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public final class DefaultBucketAssignerTest {
    private static final RuntimeException RUNTIME_EXCEPTION = new RuntimeException("Something went wrong");

    @Mock
    private BucketWriter bucketWriter;

    @Mock
    private BucketAssignerMetrics metrics;

    @Mock
    private Function<Long, OptionalLong> closeTimestampCalculator;

    private TestSweepBucketAssignerStateMachineTable stateTable;
    private DefaultBucketAssigner bucketAssigner;

    @BeforeEach
    public void before() {
        stateTable = new TestSweepBucketAssignerStateMachineTable(metrics);
        bucketAssigner = new DefaultBucketAssigner(stateTable, bucketWriter, metrics, closeTimestampCalculator);
    }

    @Test // A single call to run - i.e., we don't do one step per call where possible
    public void startStateTransitionsThroughToWaitingUntilCloseableIfBucketCannotBeClosedImmediately() {
        stateTable.setInitialState(1, BucketAssignerState.start(1));
        when(closeTimestampCalculator.apply(1L)).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        assertOperationResult(OperationResult.INCOMPLETE);
        verify(bucketWriter).writeToAllBuckets(1, Optional.empty(), TimestampRange.openBucket(1));
        verify(metrics).progressedToBucketIdentifier(1);
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(1, BucketAssignerState.opening(1)),
                BucketStateAndIdentifier.of(1, BucketAssignerState.waitingUntilCloseable(1)));
    }

    @Test
    public void openingStateWritesToAllBucketsFromNoOriginalTimestampAndTransitionsToWaitingUntilCloseable() {
        stateTable.setInitialState(1, BucketAssignerState.opening(1));
        bucketAssigner.run();

        assertOperationResult(OperationResult.INCOMPLETE);
        verify(bucketWriter).writeToAllBuckets(1, Optional.empty(), TimestampRange.openBucket(1));
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(1, BucketAssignerState.waitingUntilCloseable(1)));
    }

    @Test
    public void waitingForCloseDoesNoTransitionIfBucketCannotBeClosed() {
        stateTable.setInitialState(1, BucketAssignerState.waitingUntilCloseable(1));
        when(closeTimestampCalculator.apply(1L)).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        assertOperationResult(OperationResult.INCOMPLETE);
        verifyNoInteractions(bucketWriter);
        stateTable.assertThatStateMachineTransitionedThroughTo(); // No transition
    }

    @Test
    public void waitingForCloseTransitionsThroughClosingFromOpenToStartIfBucketCanBeClosed() {
        stateTable.setInitialState(1, BucketAssignerState.waitingUntilCloseable(1));
        when(closeTimestampCalculator.apply(1L)).thenReturn(OptionalLong.of(10));
        when(closeTimestampCalculator.apply(10L)).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        assertOperationResult(OperationResult.CLOSED, OperationResult.INCOMPLETE);
        verify(bucketWriter).writeToAllBuckets(1, Optional.of(TimestampRange.openBucket(1)), TimestampRange.of(1, 10));
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(1, BucketAssignerState.closingFromOpen(1, 10)),
                // 10 was chosen as the end timestamp to show that we correctly use that as the start, and not something
                // silly like start + 1.
                BucketStateAndIdentifier.of(2, BucketAssignerState.start(10)),
                // We successfully closed a bucket, so we'll run another iteration eagerly.
                BucketStateAndIdentifier.of(2, BucketAssignerState.opening(10)),
                BucketStateAndIdentifier.of(2, BucketAssignerState.waitingUntilCloseable(10)));
    }

    @Test
    public void closingFromOpenStateUsesOriginalStartAndEndTimestampsInsteadOfRecalculatingAndClosesBucket() {
        stateTable.setInitialState(1, BucketAssignerState.closingFromOpen(1, 2));
        // This mock is for the _next_ bucket.
        when(closeTimestampCalculator.apply(2L)).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        assertOperationResult(OperationResult.CLOSED, OperationResult.INCOMPLETE);
        verify(bucketWriter).writeToAllBuckets(1, Optional.of(TimestampRange.openBucket(1)), TimestampRange.of(1, 2));
        // We'll start the next iteration of buckets, but stop because we can't close it.
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(2, BucketAssignerState.start(2)),
                BucketStateAndIdentifier.of(2, BucketAssignerState.opening(2)),
                BucketStateAndIdentifier.of(2, BucketAssignerState.waitingUntilCloseable(2)));
        verifyNoMoreInteractions(closeTimestampCalculator);
    }

    @Test
    public void startTransitionsThroughImmediatelyClosingToStartIfBucketCanBeImmediatelyClosed() {
        stateTable.setInitialState(1, BucketAssignerState.start(1));
        when(closeTimestampCalculator.apply(1L)).thenReturn(OptionalLong.of(15));
        when(closeTimestampCalculator.apply(15L)).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        assertOperationResult(OperationResult.CLOSED, OperationResult.INCOMPLETE);
        verify(bucketWriter).writeToAllBuckets(1, Optional.empty(), TimestampRange.of(1, 15));
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(1, BucketAssignerState.immediatelyClosing(1, 15)),
                BucketStateAndIdentifier.of(2, BucketAssignerState.start(15)),
                BucketStateAndIdentifier.of(2, BucketAssignerState.opening(15)),
                BucketStateAndIdentifier.of(2, BucketAssignerState.waitingUntilCloseable(15)));
    }

    @Test
    public void immediatelyClosingUsesOriginalTimestampsInsteadOfRecalculatingAndClosesBucket() {
        stateTable.setInitialState(1, BucketAssignerState.immediatelyClosing(1, 15));
        // This mock is for the _next_ bucket.
        when(closeTimestampCalculator.apply(15L)).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        assertOperationResult(OperationResult.CLOSED, OperationResult.INCOMPLETE);
        verify(bucketWriter).writeToAllBuckets(1, Optional.empty(), TimestampRange.of(1, 15));
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(2, BucketAssignerState.start(15)),
                BucketStateAndIdentifier.of(2, BucketAssignerState.opening(15)),
                BucketStateAndIdentifier.of(2, BucketAssignerState.waitingUntilCloseable(15)));
        verifyNoMoreInteractions(closeTimestampCalculator);
    }

    @Test
    public void createsBucketsUpToMaxIterationsInOneRunIfAllCloseable() {
        stateTable.setInitialState(1, BucketAssignerState.start(1));
        when(closeTimestampCalculator.apply(1L)).thenReturn(OptionalLong.of(3));
        when(closeTimestampCalculator.apply(3L)).thenReturn(OptionalLong.of(25));
        when(closeTimestampCalculator.apply(25L)).thenReturn(OptionalLong.of(41));
        when(closeTimestampCalculator.apply(41L)).thenReturn(OptionalLong.of(114));
        when(closeTimestampCalculator.apply(114L)).thenReturn(OptionalLong.of(221));

        bucketAssigner.run();
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(1, BucketAssignerState.immediatelyClosing(1, 3)),
                BucketStateAndIdentifier.of(2, BucketAssignerState.start(3)),
                BucketStateAndIdentifier.of(2, BucketAssignerState.immediatelyClosing(3, 25)),
                BucketStateAndIdentifier.of(3, BucketAssignerState.start(25)),
                BucketStateAndIdentifier.of(3, BucketAssignerState.immediatelyClosing(25, 41)),
                BucketStateAndIdentifier.of(4, BucketAssignerState.start(41)),
                BucketStateAndIdentifier.of(4, BucketAssignerState.immediatelyClosing(41, 114)),
                BucketStateAndIdentifier.of(5, BucketAssignerState.start(114)),
                BucketStateAndIdentifier.of(5, BucketAssignerState.immediatelyClosing(114, 221)),
                BucketStateAndIdentifier.of(6, BucketAssignerState.start(221)));
        verify(metrics).progressedToBucketIdentifier(5); // We haven't entered 6 yet.

        // We should not even be trying to transition to the next state after max iterations.
        verifyNoMoreInteractions(closeTimestampCalculator);

        when(closeTimestampCalculator.apply(221L)).thenReturn(OptionalLong.empty());
        stateTable.resetInitialState(6, BucketAssignerState.start(221));
        bucketAssigner.run();
        verify(metrics).progressedToBucketIdentifier(6);
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(6, BucketAssignerState.opening(221)),
                BucketStateAndIdentifier.of(6, BucketAssignerState.waitingUntilCloseable(221)));
    }

    @ParameterizedTest(name = "State: {0}")
    @MethodSource("writingStates")
    public void statesThatWriteBucketsStayInCurrentStateIfWritesFailDueToContention(
            BucketAssignerState state, Optional<TimestampRange> expectedOldRange, TimestampRange expectedNewRange) {
        stateTable.setInitialState(1, state);
        when(bucketWriter.writeToAllBuckets(1, expectedOldRange, expectedNewRange))
                .thenReturn(WriteState.FAILED_CAS);

        bucketAssigner.run();
        assertOperationResult(OperationResult.CONTENTION_ON_WRITES);
        stateTable.assertThatStateMachineTransitionedThroughTo(); // No transition
    }

    @ParameterizedTest(name = "State: {0}")
    @MethodSource("writingStates")
    public void statesThatWriteBucketsStayInCurrentStateAndBubbleUpBucketWriteException(
            BucketAssignerState state, Optional<TimestampRange> expectedOldRange, TimestampRange expectedNewRange) {
        stateTable.setInitialState(1, state);
        when(bucketWriter.writeToAllBuckets(1, expectedOldRange, expectedNewRange))
                .thenThrow(RUNTIME_EXCEPTION);

        assertThatThrownBy(bucketAssigner::run).isEqualTo(RUNTIME_EXCEPTION);
        stateTable.assertThatStateMachineTransitionedThroughTo(); // No transition
    }

    @ParameterizedTest(name = "State: {0}")
    @MethodSource("transitionToWritingStates")
    public void transitionsToWriteStateStayInNewStateIfWritesFailDueToContention(
            BucketAssignerState state,
            Optional<TimestampRange> expectedOldRange,
            TimestampRange expectedNewRange,
            OptionalLong closeTimestamp,
            BucketAssignerState expectedNewState) {
        stateTable.setInitialState(1, state);
        when(closeTimestampCalculator.apply(1L)).thenReturn(closeTimestamp);
        when(bucketWriter.writeToAllBuckets(1, expectedOldRange, expectedNewRange))
                .thenReturn(WriteState.FAILED_CAS);

        bucketAssigner.run();
        assertOperationResult(OperationResult.CONTENTION_ON_WRITES);
        stateTable.assertThatStateMachineTransitionedThroughTo(BucketStateAndIdentifier.of(1, expectedNewState));
    }

    @ParameterizedTest(name = "State: {0}")
    @MethodSource("transitionToWritingStates")
    public void transitionsToWriteStateStayInNewStateIfWritesFailDueToException(
            BucketAssignerState state,
            Optional<TimestampRange> expectedOldRange,
            TimestampRange expectedNewRange,
            OptionalLong closeTimestamp,
            BucketAssignerState expectedNewState) {
        stateTable.setInitialState(1, state);
        when(closeTimestampCalculator.apply(1L)).thenReturn(closeTimestamp);
        when(bucketWriter.writeToAllBuckets(1, expectedOldRange, expectedNewRange))
                .thenThrow(RUNTIME_EXCEPTION);

        assertThatThrownBy(bucketAssigner::run).isEqualTo(RUNTIME_EXCEPTION);
        stateTable.assertThatStateMachineTransitionedThroughTo(BucketStateAndIdentifier.of(1, expectedNewState));
    }

    @ParameterizedTest(name = "State: {0}")
    @MethodSource("transitionToWritingStates")
    public void transitionsToWriteStateStayInNewStateIfBucketWriteThrowsException(
            BucketAssignerState state,
            Optional<TimestampRange> expectedOldRange,
            TimestampRange expectedNewRange,
            OptionalLong closeTimestamp,
            BucketAssignerState expectedNewState) {
        stateTable.setInitialState(1, state);
        when(closeTimestampCalculator.apply(1L)).thenReturn(closeTimestamp);
        when(bucketWriter.writeToAllBuckets(1, expectedOldRange, expectedNewRange))
                .thenThrow(RUNTIME_EXCEPTION);

        assertThatThrownBy(bucketAssigner::run).isEqualTo(RUNTIME_EXCEPTION);
        stateTable.assertThatStateMachineTransitionedThroughTo(BucketStateAndIdentifier.of(1, expectedNewState));
    }

    private static Stream<Arguments> writingStates() {
        return Stream.of(
                Arguments.of(BucketAssignerState.opening(1), Optional.empty(), TimestampRange.openBucket(1)),
                Arguments.of(
                        BucketAssignerState.closingFromOpen(1, 2),
                        Optional.of(TimestampRange.openBucket(1)),
                        TimestampRange.of(1, 2)),
                Arguments.of(
                        BucketAssignerState.immediatelyClosing(1, 15), Optional.empty(), TimestampRange.of(1, 15)));
    }

    private static Stream<Arguments> transitionToWritingStates() {
        return Stream.of(
                Arguments.of(
                        BucketAssignerState.start(1),
                        Optional.empty(),
                        TimestampRange.openBucket(1),
                        OptionalLong.empty(),
                        BucketAssignerState.opening(1)),
                Arguments.of(
                        BucketAssignerState.start(1),
                        Optional.empty(),
                        TimestampRange.of(1, 2),
                        OptionalLong.of(2),
                        BucketAssignerState.immediatelyClosing(1, 2)),
                Arguments.of(
                        BucketAssignerState.waitingUntilCloseable(1),
                        Optional.of(TimestampRange.openBucket(1)),
                        TimestampRange.of(1, 3),
                        OptionalLong.of(3),
                        BucketAssignerState.closingFromOpen(1, 3)));
    }

    private void assertOperationResult(OperationResult... results) {
        InOrder inOrder = inOrder(metrics);
        for (OperationResult result : results) {
            inOrder.verify(metrics).operationResultForIteration(result);
        }
        // Not called any more times.
        // We can't use verifyNoMoreInteractions, because that would require verifying no other interactions on the
        // whole metrics object, which is not what we want.
        verify(metrics, times(results.length)).operationResultForIteration(any());
    }

    private static final class TestSweepBucketAssignerStateMachineTable
            implements SweepBucketAssignerStateMachineTable {
        private final List<BucketStateAndIdentifier> stateTransitions = new ArrayList<>();
        private final BucketAssignerMetrics metrics;

        TestSweepBucketAssignerStateMachineTable(BucketAssignerMetrics metrics) {
            this.metrics = metrics;
        }

        @Override
        public void updateStateMachineForBucketAssigner(
                BucketStateAndIdentifier original, BucketStateAndIdentifier updated) {
            assertThat(stateTransitions).last().isEqualTo(original);
            stateTransitions.add(updated);
        }

        @Override
        public BucketStateAndIdentifier getBucketStateAndIdentifier() {
            return stateTransitions.get(stateTransitions.size() - 1);
        }

        public void setInitialState(long bucketIdentifier, BucketAssignerState state) {
            assertThat(stateTransitions).isEmpty();
            stateTransitions.add(BucketStateAndIdentifier.of(bucketIdentifier, state));
        }

        public void assertThatStateMachineTransitionedThroughTo(BucketStateAndIdentifier... states) {
            if (states.length == 0) {
                assertThat(stateTransitions).hasSize(1); // first state
                verify(metrics, times(1)).bucketAssignerState(any()); // first state
                return;
            }
            List<BucketStateAndIdentifier> transitionedStatesNotIncludingInitial =
                    stateTransitions.subList(1, stateTransitions.size());
            assertThat(transitionedStatesNotIncludingInitial).containsExactly(states);
            InOrder inOrder = inOrder(metrics);
            // We can include the first state, since we would have recorded a metric on entry, but not the last state.
            for (BucketStateAndIdentifier state : transitionedStatesNotIncludingInitial.subList(0, states.length - 1)) {
                inOrder.verify(metrics).bucketAssignerState(state.state());
            }
        }

        // Used if we run multiple iterations in a single state - call this to reset the current starting state,
        // otherwise if you assert the states, you'll get states from the previous run in the assertion
        public void resetInitialState(long bucketIdentifier, BucketAssignerState newInitialState) {
            stateTransitions.clear();
            stateTransitions.add(BucketStateAndIdentifier.of(bucketIdentifier, newInitialState));
        }
    }
}
