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

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.CheckAndSetException;
import com.palantir.atlasdb.sweep.asts.TimestampRange;
import com.palantir.atlasdb.sweep.asts.bucketingthings.BucketWriter.WriteState;
import com.palantir.atlasdb.sweep.asts.bucketingthings.DefaultBucketAssigner.IterationResult.OperationResult;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import org.immutables.value.Value;

public final class DefaultBucketAssigner {
    private static final SafeLogger log = SafeLoggerFactory.get(DefaultBucketAssigner.class);
    private static final int MAX_BUCKETS_PER_ITERATION = 5;

    private final SweepBucketAssignerStateMachineTable sweepBucketAssignerStateMachineTable;
    private final BucketWriter bucketWriter;
    private final SweepBucketRecordsTable sweepBucketRecordsTable;
    private final BucketAssignerEventHandler metrics;
    private final Function<Long, OptionalLong> closeTimestampCalculator;

    @VisibleForTesting
    DefaultBucketAssigner(
            SweepBucketAssignerStateMachineTable sweepBucketAssignerStateMachineTable,
            BucketWriter bucketWriter,
            SweepBucketRecordsTable sweepBucketRecordsTable,
            BucketAssignerEventHandler metrics,
            Function<Long, OptionalLong> closeTimestampCalculator) {
        this.sweepBucketAssignerStateMachineTable = sweepBucketAssignerStateMachineTable;
        this.bucketWriter = bucketWriter;
        this.sweepBucketRecordsTable = sweepBucketRecordsTable;
        this.metrics = metrics;
        this.closeTimestampCalculator = closeTimestampCalculator;
    }

    public static DefaultBucketAssigner create(
            SweepBucketAssignerStateMachineTable sweepBucketAssignerStateMachineTable,
            BucketWriter bucketWriter,
            SweepBucketRecordsTable sweepBucketRecordsTable,
            BucketAssignerEventHandler metrics,
            Function<Long, OptionalLong> closeTimestampCalculator) {
        return new DefaultBucketAssigner(
                sweepBucketAssignerStateMachineTable,
                bucketWriter,
                sweepBucketRecordsTable,
                metrics,
                closeTimestampCalculator);
    }

    public void run() {
        // TODO(mdaudali): Take a lock, or use Lockable<ExclusiveTask> to externalise the locking.
        //  then, this may end up implementing Runnable.
        IterationResult iterationResult = createNewBucket();

        // We cap the number of buckets we create per iteration to avoid a single thread getting stuck creating
        // buckets. This is a safety measure in case there's a bug in the bucket creation logic that causes it to
        // infinitely create the same bucket or return CLOSED over and over again.
        for (int i = 1;
                i < MAX_BUCKETS_PER_ITERATION && iterationResult.operationResult() == OperationResult.CLOSED;
                i++) {
            iterationResult = createNewBucket();
            if (i == MAX_BUCKETS_PER_ITERATION - 1 && iterationResult.operationResult() == OperationResult.CLOSED) {
                metrics.createdMaxBucketsInSingleIteration();
                log.warn(
                        "Reached the maximum number of buckets created per iteration. This is most likely because"
                            + " we're very far behind, or because there's a bug in the bucket creation logic that"
                            + " causes it to infinitely create the same bucket or return CLOSED over and over again."
                            + " The last bucket processed has ID {} and reached state {}",
                        SafeArg.of("bucketIdentifier", iterationResult.bucketIdentifier()),
                        SafeArg.of("bucketAssignerState", iterationResult.bucketAssignerState()));
            }
        }
    }

    private IterationResult createNewBucket() {
        BucketStateAndIdentifier stateAndIdentifier =
                sweepBucketAssignerStateMachineTable.getBucketStateAndIdentifier();
        BucketStateVisitor visitor = new BucketStateVisitor(stateAndIdentifier.bucketIdentifier());
        IterationResult iterationResult = stateAndIdentifier.state().accept(visitor);
        metrics.operationResultForIteration(iterationResult.operationResult());
        log.info(
                "Processed a bucket with ID {} and reached state {}",
                SafeArg.of("bucketIdentifier", iterationResult.bucketIdentifier()),
                SafeArg.of("bucketAssignerState", iterationResult.bucketAssignerState()));
        return iterationResult;
    }

    // If we've called this method, it's because we've successfully written all the buckets without detecting contention
    // with another writer. It's possible that a previous write attempt did also write to the records table and failed
    // before updating the state machine, and so we need to ignore the exception for that case.
    // If there's active contention that we didn't detect, it's still safe to ignore the CAS _because_ the writes
    // to this table are set in advance by the state machine, and so both writers must be proposing the same value.
    private void tryWriteBucketRecordIgnoringCasException(long bucketIdentifier, TimestampRange timestampRange) {
        try {
            sweepBucketRecordsTable.putTimestampRangeRecord(bucketIdentifier, timestampRange);
        } catch (CheckAndSetException e) {
            log.warn(
                    "Failed to write bucket record for bucket {} with timestamp range {} due to a CAS exception."
                            + " This is likely due to a previous attempt failing to update the state machine state"
                            + " after having written the record.",
                    SafeArg.of("bucketIdentifier", bucketIdentifier),
                    SafeArg.of("timestampRange", timestampRange),
                    e);
        }
    }

    private final class BucketStateVisitor implements BucketAssignerState.Visitor<IterationResult> {
        private final long bucketIdentifier;

        private BucketStateVisitor(long bucketIdentifier) {
            this.bucketIdentifier = bucketIdentifier;
        }

        @Override
        public IterationResult visit(BucketAssignerState.Start start) {
            metrics.progressedToBucketIdentifier(bucketIdentifier);
            metrics.bucketAssignerState(start);
            OptionalLong closeTimestamp = closeTimestampCalculator.apply(start.startTimestampInclusive());

            if (closeTimestamp.isPresent()) {
                return tryTransitionAndExecuteNextState(
                        start,
                        BucketAssignerState.immediatelyClosing(
                                start.startTimestampInclusive(), closeTimestamp.orElseThrow()));
            } else {
                return tryTransitionAndExecuteNextState(
                        start, BucketAssignerState.opening(start.startTimestampInclusive()));
            }
        }

        @Override
        public IterationResult visit(BucketAssignerState.Opening open) {
            metrics.bucketAssignerState(open);
            WriteState writeState = bucketWriter.writeToAllBuckets(
                    bucketIdentifier, Optional.empty(), TimestampRange.openBucket(open.startTimestampInclusive()));

            if (writeState == WriteState.FAILED_CAS) {
                return IterationResult.of(OperationResult.CONTENTION_ON_WRITES, bucketIdentifier, open);
            }

            BucketAssignerState waitingUntilCloseable =
                    BucketAssignerState.waitingUntilCloseable(open.startTimestampInclusive());
            sweepBucketAssignerStateMachineTable.updateStateMachineForBucketAssigner(
                    BucketStateAndIdentifier.of(bucketIdentifier, open),
                    BucketStateAndIdentifier.of(bucketIdentifier, waitingUntilCloseable));
            return IterationResult.of(OperationResult.INCOMPLETE, bucketIdentifier, waitingUntilCloseable);
        }

        @Override
        public IterationResult visit(BucketAssignerState.WaitingUntilCloseable waitingClose) {
            metrics.bucketAssignerState(waitingClose);
            OptionalLong closeTimestamp = closeTimestampCalculator.apply(waitingClose.startTimestampInclusive());

            if (closeTimestamp.isPresent()) {
                return tryTransitionAndExecuteNextState(
                        waitingClose,
                        BucketAssignerState.closingFromOpen(
                                waitingClose.startTimestampInclusive(), closeTimestamp.orElseThrow()));
            } else {
                return IterationResult.of(OperationResult.INCOMPLETE, bucketIdentifier, waitingClose);
            }
        }

        @Override
        public IterationResult visit(BucketAssignerState.ClosingFromOpen closingFromOpen) {
            metrics.bucketAssignerState(closingFromOpen);
            return tryTransitionToStartAfterClosingBuckets(
                    Optional.of(TimestampRange.openBucket(closingFromOpen.startTimestampInclusive())),
                    TimestampRange.of(
                            closingFromOpen.startTimestampInclusive(), closingFromOpen.endTimestampExclusive()),
                    closingFromOpen);
        }

        @Override
        public IterationResult visit(BucketAssignerState.ImmediatelyClosing immediateClose) {
            metrics.bucketAssignerState(immediateClose);
            return tryTransitionToStartAfterClosingBuckets(
                    Optional.empty(),
                    TimestampRange.of(immediateClose.startTimestampInclusive(), immediateClose.endTimestampExclusive()),
                    immediateClose);
        }

        private IterationResult tryTransitionToStartAfterClosingBuckets(
                Optional<TimestampRange> oldTimestampRange,
                TimestampRange newTimestampRange,
                BucketAssignerState lastState) {
            WriteState writeState =
                    bucketWriter.writeToAllBuckets(bucketIdentifier, oldTimestampRange, newTimestampRange);

            if (writeState == WriteState.FAILED_CAS) {
                return IterationResult.of(OperationResult.CONTENTION_ON_WRITES, bucketIdentifier, lastState);
            }
            tryWriteBucketRecordIgnoringCasException(bucketIdentifier, newTimestampRange);

            BucketAssignerState start = BucketAssignerState.start(newTimestampRange.endExclusive());
            sweepBucketAssignerStateMachineTable.updateStateMachineForBucketAssigner(
                    BucketStateAndIdentifier.of(bucketIdentifier, lastState),
                    BucketStateAndIdentifier.of(bucketIdentifier + 1, start));
            return IterationResult.of(OperationResult.CLOSED, bucketIdentifier, start);
        }

        private IterationResult tryTransitionAndExecuteNextState(
                BucketAssignerState currentState, BucketAssignerState newState) {
            sweepBucketAssignerStateMachineTable.updateStateMachineForBucketAssigner(
                    BucketStateAndIdentifier.of(bucketIdentifier, currentState),
                    BucketStateAndIdentifier.of(bucketIdentifier, newState));
            return newState.accept(this);
        }
    }

    @Value.Immutable
    interface IterationResult {
        @Value.Parameter
        OperationResult operationResult();

        @Value.Parameter
        long bucketIdentifier();

        @Value.Parameter
        BucketAssignerState bucketAssignerState();

        // TODO(mdaudali): Re-evaluate if this is a useful metric or not.
        enum OperationResult {
            // The bucket is still open (we didn't close it in this iteration)
            INCOMPLETE,
            // We could close the bucket in this iteration
            CLOSED,
            // We believe someone else is also updating the buckets, so we're stopping here.
            CONTENTION_ON_WRITES
        }

        static IterationResult of(
                OperationResult operationResult, long bucketIdentifier, BucketAssignerState bucketAssignerState) {
            return ImmutableIterationResult.of(operationResult, bucketIdentifier, bucketAssignerState);
        }
    }

    // TODO(mdaudali): This is a placeholder. I imagine that we may also use these consumers to inform autoscaling
    public interface BucketAssignerEventHandler {
        void progressedToBucketIdentifier(long bucketIdentifier);

        void operationResultForIteration(OperationResult iterationStateOutcome);

        void createdMaxBucketsInSingleIteration();

        void bucketAssignerState(BucketAssignerState bucketAssignerState);
    }
}
