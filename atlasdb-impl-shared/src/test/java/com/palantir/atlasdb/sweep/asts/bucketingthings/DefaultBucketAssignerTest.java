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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.sweep.asts.TimestampRange;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketWriter.WriteState;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketAssigner.BucketAssignerEventHandler;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketAssigner.IterationResult.OperationResult;
import com.palantir.atlasdb.sweep.queue.SweepQueueUtils;
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
    private SweepBucketRecordsTable sweepBucketRecordsTable;

    @Mock
    private BucketAssignerEventHandler eventHandler;

    @Mock
    private Function<Long, OptionalLong> closeTimestampCalculator;

    private TestSweepBucketAssignerStateMachineTable stateTable;
    private DefaultBucketAssigner bucketAssigner;

    @BeforeEach
    public void before() {
        stateTable = new TestSweepBucketAssignerStateMachineTable(eventHandler);
        bucketAssigner = new DefaultBucketAssigner(
                stateTable, bucketWriter, sweepBucketRecordsTable, eventHandler, closeTimestampCalculator);
    }

    @Test // A single call to run - i.e., we don't do one step per call where possible
    public void startStateTransitionsThroughToWaitingUntilCloseableIfBucketCannotBeClosedImmediately() {
        stateTable.setInitialState(1, BucketAssignerState.start(coarsePartitionTimestamp(1)));
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(1))).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        assertOperationResult(OperationResult.INCOMPLETE);
        verify(bucketWriter)
                .writeToAllBuckets(1, Optional.empty(), TimestampRange.openBucket(coarsePartitionTimestamp(1)));
        verifyNoInteractions(sweepBucketRecordsTable);
        verify(eventHandler).progressedToBucketIdentifier(1);
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(1, BucketAssignerState.opening(coarsePartitionTimestamp(1))),
                BucketStateAndIdentifier.of(1, BucketAssignerState.waitingUntilCloseable(coarsePartitionTimestamp(1))));
    }

    @Test
    public void openingStateWritesToAllBucketsFromNoOriginalTimestampAndTransitionsToWaitingUntilCloseable() {
        stateTable.setInitialState(1, BucketAssignerState.opening(coarsePartitionTimestamp(2)));
        bucketAssigner.run();

        assertOperationResult(OperationResult.INCOMPLETE);
        verify(bucketWriter)
                .writeToAllBuckets(1, Optional.empty(), TimestampRange.openBucket(coarsePartitionTimestamp(2)));
        verifyNoInteractions(sweepBucketRecordsTable);
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(1, BucketAssignerState.waitingUntilCloseable(coarsePartitionTimestamp(2))));
    }

    @Test
    public void waitingForCloseDoesNoTransitionIfBucketCannotBeClosed() {
        stateTable.setInitialState(1, BucketAssignerState.waitingUntilCloseable(coarsePartitionTimestamp(3)));
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(3))).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        assertOperationResult(OperationResult.INCOMPLETE);
        verifyNoInteractions(bucketWriter);
        verifyNoInteractions(sweepBucketRecordsTable);
        stateTable.assertThatStateMachineTransitionedThroughTo(); // No transition
    }

    @Test
    public void waitingForCloseTransitionsThroughClosingFromOpenToStartIfBucketCanBeClosedAndWritesRecord() {
        stateTable.setInitialState(1, BucketAssignerState.waitingUntilCloseable(coarsePartitionTimestamp(1)));
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(1)))
                .thenReturn(OptionalLong.of(coarsePartitionTimestamp(10)));
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(10))).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        assertOperationResult(OperationResult.CLOSED, OperationResult.INCOMPLETE);
        verify(bucketWriter)
                .writeToAllBuckets(
                        1,
                        Optional.of(TimestampRange.openBucket(coarsePartitionTimestamp(1))),
                        TimestampRange.of(coarsePartitionTimestamp(1), coarsePartitionTimestamp(10)));
        verify(sweepBucketRecordsTable)
                .putTimestampRangeRecord(
                        1, TimestampRange.of(coarsePartitionTimestamp(1), coarsePartitionTimestamp(10)));
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(
                        1,
                        BucketAssignerState.closingFromOpen(coarsePartitionTimestamp(1), coarsePartitionTimestamp(10))),
                // 10 was chosen as the end timestamp to show that we correctly use that as the start, and not something
                // silly like start + 1.
                BucketStateAndIdentifier.of(2, BucketAssignerState.start(coarsePartitionTimestamp(10))),
                // We successfully closed a bucket, so we'll run another iteration eagerly.
                BucketStateAndIdentifier.of(2, BucketAssignerState.opening(coarsePartitionTimestamp(10))),
                BucketStateAndIdentifier.of(
                        2, BucketAssignerState.waitingUntilCloseable(coarsePartitionTimestamp(10))));
    }

    @Test
    public void
            closingFromOpenStateUsesOriginalStartAndEndTimestampsInsteadOfRecalculatingAndClosesBucketAndWritesRecord() {
        stateTable.setInitialState(
                1, BucketAssignerState.closingFromOpen(coarsePartitionTimestamp(3), coarsePartitionTimestamp(4)));
        // This mock is for the _next_ bucket.
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(4))).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        assertOperationResult(OperationResult.CLOSED, OperationResult.INCOMPLETE);
        verify(bucketWriter)
                .writeToAllBuckets(
                        1,
                        Optional.of(TimestampRange.openBucket(coarsePartitionTimestamp(3))),
                        TimestampRange.of(coarsePartitionTimestamp(3), coarsePartitionTimestamp(4)));
        verify(sweepBucketRecordsTable)
                .putTimestampRangeRecord(
                        1, TimestampRange.of(coarsePartitionTimestamp(3), coarsePartitionTimestamp(4)));
        // We'll start the next iteration of buckets, but stop because we can't close it.
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(2, BucketAssignerState.start(coarsePartitionTimestamp(4))),
                BucketStateAndIdentifier.of(2, BucketAssignerState.opening(coarsePartitionTimestamp(4))),
                BucketStateAndIdentifier.of(2, BucketAssignerState.waitingUntilCloseable(coarsePartitionTimestamp(4))));
        verifyNoMoreInteractions(closeTimestampCalculator);
    }

    @Test
    public void startTransitionsThroughImmediatelyClosingToStartIfBucketCanBeImmediatelyClosedAndWritesRecord() {
        stateTable.setInitialState(1, BucketAssignerState.start(coarsePartitionTimestamp(1)));
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(1)))
                .thenReturn(OptionalLong.of(coarsePartitionTimestamp(15)));
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(15))).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        assertOperationResult(OperationResult.CLOSED, OperationResult.INCOMPLETE);
        verify(bucketWriter)
                .writeToAllBuckets(
                        1,
                        Optional.empty(),
                        TimestampRange.of(coarsePartitionTimestamp(1), coarsePartitionTimestamp(15)));
        verify(sweepBucketRecordsTable)
                .putTimestampRangeRecord(
                        1, TimestampRange.of(coarsePartitionTimestamp(1), coarsePartitionTimestamp(15)));
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(
                        1,
                        BucketAssignerState.immediatelyClosing(
                                coarsePartitionTimestamp(1), coarsePartitionTimestamp(15))),
                BucketStateAndIdentifier.of(2, BucketAssignerState.start(coarsePartitionTimestamp(15))),
                BucketStateAndIdentifier.of(2, BucketAssignerState.opening(coarsePartitionTimestamp(15))),
                BucketStateAndIdentifier.of(
                        2, BucketAssignerState.waitingUntilCloseable(coarsePartitionTimestamp(15))));
    }

    @Test
    public void immediatelyClosingUsesOriginalTimestampsInsteadOfRecalculatingAndClosesBucketAndWritesRecord() {
        stateTable.setInitialState(
                1, BucketAssignerState.immediatelyClosing(coarsePartitionTimestamp(1), coarsePartitionTimestamp(15)));
        // This mock is for the _next_ bucket.
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(15))).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        assertOperationResult(OperationResult.CLOSED, OperationResult.INCOMPLETE);
        verify(bucketWriter)
                .writeToAllBuckets(
                        1,
                        Optional.empty(),
                        TimestampRange.of(coarsePartitionTimestamp(1), coarsePartitionTimestamp(15)));
        verify(sweepBucketRecordsTable)
                .putTimestampRangeRecord(
                        1, TimestampRange.of(coarsePartitionTimestamp(1), coarsePartitionTimestamp(15)));
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(2, BucketAssignerState.start(coarsePartitionTimestamp(15))),
                BucketStateAndIdentifier.of(2, BucketAssignerState.opening(coarsePartitionTimestamp(15))),
                BucketStateAndIdentifier.of(
                        2, BucketAssignerState.waitingUntilCloseable(coarsePartitionTimestamp(15))));
        verifyNoMoreInteractions(closeTimestampCalculator);
    }

    @Test
    public void createsBucketsUpToMaxIterationsInOneRunIfAllCloseable() {
        stateTable.setInitialState(1, BucketAssignerState.start(coarsePartitionTimestamp(1)));
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(1)))
                .thenReturn(OptionalLong.of(coarsePartitionTimestamp(3)));
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(3)))
                .thenReturn(OptionalLong.of(coarsePartitionTimestamp(25)));
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(25)))
                .thenReturn(OptionalLong.of(coarsePartitionTimestamp(41)));
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(41)))
                .thenReturn(OptionalLong.of(coarsePartitionTimestamp(114)));
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(114)))
                .thenReturn(OptionalLong.of(coarsePartitionTimestamp(221)));

        bucketAssigner.run();
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(
                        1,
                        BucketAssignerState.immediatelyClosing(
                                coarsePartitionTimestamp(1), coarsePartitionTimestamp(3))),
                BucketStateAndIdentifier.of(2, BucketAssignerState.start(coarsePartitionTimestamp(3))),
                BucketStateAndIdentifier.of(
                        2,
                        BucketAssignerState.immediatelyClosing(
                                coarsePartitionTimestamp(3), coarsePartitionTimestamp(25))),
                BucketStateAndIdentifier.of(3, BucketAssignerState.start(coarsePartitionTimestamp(25))),
                BucketStateAndIdentifier.of(
                        3,
                        BucketAssignerState.immediatelyClosing(
                                coarsePartitionTimestamp(25), coarsePartitionTimestamp(41))),
                BucketStateAndIdentifier.of(4, BucketAssignerState.start(coarsePartitionTimestamp(41))),
                BucketStateAndIdentifier.of(
                        4,
                        BucketAssignerState.immediatelyClosing(
                                coarsePartitionTimestamp(41), coarsePartitionTimestamp(114))),
                BucketStateAndIdentifier.of(5, BucketAssignerState.start(coarsePartitionTimestamp(114))),
                BucketStateAndIdentifier.of(
                        5,
                        BucketAssignerState.immediatelyClosing(
                                coarsePartitionTimestamp(114), coarsePartitionTimestamp(221))),
                BucketStateAndIdentifier.of(6, BucketAssignerState.start(coarsePartitionTimestamp(221))));
        verify(eventHandler).progressedToBucketIdentifier(5); // We haven't entered 6 yet.

        // We should not even be trying to transition to the next state after max iterations.
        verifyNoMoreInteractions(closeTimestampCalculator);

        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(221))).thenReturn(OptionalLong.empty());
        stateTable.resetInitialState(6, BucketAssignerState.start(coarsePartitionTimestamp(221)));
        bucketAssigner.run();
        verify(eventHandler).progressedToBucketIdentifier(6);
        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(6, BucketAssignerState.opening(coarsePartitionTimestamp(221))),
                BucketStateAndIdentifier.of(
                        6, BucketAssignerState.waitingUntilCloseable(coarsePartitionTimestamp(221))));
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
        verifyNoInteractions(sweepBucketRecordsTable);
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
        verifyNoInteractions(sweepBucketRecordsTable);
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
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(1))).thenReturn(closeTimestamp);
        when(bucketWriter.writeToAllBuckets(1, expectedOldRange, expectedNewRange))
                .thenReturn(WriteState.FAILED_CAS);

        bucketAssigner.run();
        assertOperationResult(OperationResult.CONTENTION_ON_WRITES);
        stateTable.assertThatStateMachineTransitionedThroughTo(BucketStateAndIdentifier.of(1, expectedNewState));
        verifyNoInteractions(sweepBucketRecordsTable);
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
        when(closeTimestampCalculator.apply(coarsePartitionTimestamp(1))).thenReturn(closeTimestamp);
        when(bucketWriter.writeToAllBuckets(1, expectedOldRange, expectedNewRange))
                .thenThrow(RUNTIME_EXCEPTION);

        assertThatThrownBy(bucketAssigner::run).isEqualTo(RUNTIME_EXCEPTION);
        stateTable.assertThatStateMachineTransitionedThroughTo(BucketStateAndIdentifier.of(1, expectedNewState));
        verifyNoInteractions(sweepBucketRecordsTable);
    }

    @ParameterizedTest(name = "State: {0}")
    @MethodSource("closingStates")
    public void stateMachineStillProgressesToStartStateIfWritingRecordThrowsCheckAndSetException(
            BucketAssignerState state, TimestampRange timestampRange) {
        stateTable.setInitialState(1, state);
        doThrow(new CheckAndSetException("foo"))
                .when(sweepBucketRecordsTable)
                .putTimestampRangeRecord(1, timestampRange);

        long nextStartTimestamp = timestampRange.endExclusive();
        when(closeTimestampCalculator.apply(nextStartTimestamp)).thenReturn(OptionalLong.empty());
        bucketAssigner.run();

        stateTable.assertThatStateMachineTransitionedThroughTo(
                BucketStateAndIdentifier.of(2, BucketAssignerState.start(nextStartTimestamp)),
                BucketStateAndIdentifier.of(2, BucketAssignerState.opening(nextStartTimestamp)),
                BucketStateAndIdentifier.of(2, BucketAssignerState.waitingUntilCloseable(nextStartTimestamp)));
    }

    private static Stream<Arguments> writingStates() {
        return Stream.of(
                Arguments.of(
                        BucketAssignerState.opening(coarsePartitionTimestamp(1)),
                        Optional.empty(),
                        TimestampRange.openBucket(coarsePartitionTimestamp(1))),
                Arguments.of(
                        BucketAssignerState.closingFromOpen(coarsePartitionTimestamp(1), coarsePartitionTimestamp(2)),
                        Optional.of(TimestampRange.openBucket(coarsePartitionTimestamp(1))),
                        TimestampRange.of(coarsePartitionTimestamp(1), coarsePartitionTimestamp(2))),
                Arguments.of(
                        BucketAssignerState.immediatelyClosing(
                                coarsePartitionTimestamp(1), coarsePartitionTimestamp(15)),
                        Optional.empty(),
                        TimestampRange.of(coarsePartitionTimestamp(1), coarsePartitionTimestamp(15))));
    }

    private static Stream<Arguments> transitionToWritingStates() {
        return Stream.of(
                Arguments.of(
                        BucketAssignerState.start(coarsePartitionTimestamp(1)),
                        Optional.empty(),
                        TimestampRange.openBucket(coarsePartitionTimestamp(1)),
                        OptionalLong.empty(),
                        BucketAssignerState.opening(coarsePartitionTimestamp(1))),
                Arguments.of(
                        BucketAssignerState.start(coarsePartitionTimestamp(1)),
                        Optional.empty(),
                        TimestampRange.of(coarsePartitionTimestamp(1), coarsePartitionTimestamp(2)),
                        OptionalLong.of(coarsePartitionTimestamp(2)),
                        BucketAssignerState.immediatelyClosing(
                                coarsePartitionTimestamp(1), coarsePartitionTimestamp(2))),
                Arguments.of(
                        BucketAssignerState.waitingUntilCloseable(coarsePartitionTimestamp(1)),
                        Optional.of(TimestampRange.openBucket(coarsePartitionTimestamp(1))),
                        TimestampRange.of(coarsePartitionTimestamp(1), coarsePartitionTimestamp(3)),
                        OptionalLong.of(coarsePartitionTimestamp(3)),
                        BucketAssignerState.closingFromOpen(coarsePartitionTimestamp(1), coarsePartitionTimestamp(3))));
    }

    private static Stream<Arguments> closingStates() {
        return Stream.of(
                Arguments.of(
                        BucketAssignerState.closingFromOpen(coarsePartitionTimestamp(1), coarsePartitionTimestamp(2)),
                        TimestampRange.of(coarsePartitionTimestamp(1), coarsePartitionTimestamp(2))),
                Arguments.of(
                        BucketAssignerState.immediatelyClosing(
                                coarsePartitionTimestamp(32), coarsePartitionTimestamp(150)),
                        TimestampRange.of(coarsePartitionTimestamp(32), coarsePartitionTimestamp(150))));
    }

    private void assertOperationResult(OperationResult... results) {
        InOrder inOrder = inOrder(eventHandler);
        for (OperationResult result : results) {
            inOrder.verify(eventHandler).operationResultForIteration(result);
        }
        // Not called any more times.
        // We can't use verifyNoMoreInteractions, because that would require verifying no other interactions on the
        // whole metrics object, which is not what we want.
        verify(eventHandler, times(results.length)).operationResultForIteration(any());
    }

    private static long coarsePartitionTimestamp(long coarsePartition) {
        return SweepQueueUtils.minTsForCoarsePartition(coarsePartition);
    }

    private static final class TestSweepBucketAssignerStateMachineTable
            implements SweepBucketAssignerStateMachineTable {
        private final List<BucketStateAndIdentifier> stateTransitions = new ArrayList<>();
        private final BucketAssignerEventHandler metrics;

        TestSweepBucketAssignerStateMachineTable(BucketAssignerEventHandler metrics) {
            this.metrics = metrics;
        }

        @Override
        public void setInitialStateForBucketAssigner(BucketAssignerState initialState) {
            throw new UnsupportedOperationException("This should not be called");
        }

        @Override
        public boolean doesStateMachineStateExist() {
            throw new UnsupportedOperationException("This should not be called");
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
