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

package com.palantir.atlasdb.transaction.impl.logging;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import com.palantir.logsafe.SafeArg;

public class CommitProfileProcessor {
    private final LogConsumerProcessor logSink;
    private final Supplier<Timer> nonPutOverheadTimerSupplier;
    private final Supplier<Histogram> nonPutOverheadMillionthsHistogramSupplier;

    public CommitProfileProcessor(
            LogConsumerProcessor logSink,
            Supplier<Timer> nonPutOverheadTimerSupplier,
            Supplier<Histogram> nonPutOverheadMillionthsHistogramSupplier) {
        this.logSink = logSink;
        this.nonPutOverheadTimerSupplier = nonPutOverheadTimerSupplier;
        this.nonPutOverheadMillionthsHistogramSupplier = nonPutOverheadMillionthsHistogramSupplier;
    }

    public void consumeProfilingData(
            TransactionCommitProfile profile,
            Set<TableReference> tablesWrittenTo,
            long byteCount,
            long postCommitOverhead) {
        maybeLogToSink(profile, tablesWrittenTo, byteCount, postCommitOverhead);
        updateNonPutOverheadMetrics(profile, postCommitOverhead);
    }

    private void maybeLogToSink(TransactionCommitProfile profile, Set<TableReference> tables, long byteCount,
            long postCommitOverhead) {
        logSink.maybeLog(() -> {
            LoggingArgs.SafeAndUnsafeTableReferences tableRefs = LoggingArgs.tableRefs(tables);
            return ImmutableLogTemplate.builder().format(
                    "Committed {} bytes with locks, start ts {}, commit ts {}, "
                            + "acquiring locks took {} μs, checking for conflicts took {} μs, "
                            + "writing to the sweep queue took {} μs, "
                            + "writing data took {} μs, "
                            + "getting the commit timestamp took {} μs, punch took {} μs, "
                            + "serializable r/w conflict check took {} μs, putCommitTs took {} μs, "
                            + "pre-commit lock checks took {} μs, user pre-commit conditions took {} μs, "
                            + "total time spent committing writes was {} μs, "
                            + "post-commit intra-transaction cleanup took {} μs, "
                            + "total time since tx creation {} μs, tables: {}, {}.")
                    .arguments(
                            SafeArg.of("numBytes", byteCount),
                            SafeArg.of("startTs", profile.startTimestamp()),
                            SafeArg.of("commitTs", profile.commitTimestamp()),
                            SafeArg.of("microsForLocks", profile.acquireRowLocksMicros()),
                            SafeArg.of("microsCheckForConflicts", profile.conflictCheckMicros()),
                            SafeArg.of("microsWritingToTargetedSweepQueue",
                                    profile.writingToSweepQueueMicros()),
                            SafeArg.of("microsForWrites", profile.keyValueServiceWriteMicros()),
                            SafeArg.of("microsForGetCommitTs", profile.getCommitTimestampMicros()),
                            SafeArg.of("microsForPunch", profile.punchMicros()),
                            SafeArg.of("microsForReadWriteConflictCheck",
                                    profile.readWriteConflictCheckMicros()),
                            SafeArg.of("microsForPutCommitTs", profile.putCommitTimestampMicros()),
                            SafeArg.of("microsForPreCommitLockCheck",
                                    profile.verifyPreCommitLockCheckMicros()),
                            SafeArg.of("microsForUserPreCommitCondition",
                                    profile.verifyUserPreCommitConditionMicros()),
                            SafeArg.of("microsForCommitStage", profile.totalCommitStageMicros()),
                            SafeArg.of("microsForPostCommitOverhead", postCommitOverhead),
                            SafeArg.of("microsSinceCreation", profile.totalTimeSinceTransactionCreationMicros()),
                            tableRefs.safeTableRefs(),
                            tableRefs.unsafeTableRefs())
                    .build();
        });
    }

    private void updateNonPutOverheadMetrics(TransactionCommitProfile profile, long postCommitOverhead) {
        long nonPutOverhead = getNonPutOverhead(profile, postCommitOverhead);
        nonPutOverheadTimerSupplier.get().update(nonPutOverhead, TimeUnit.MICROSECONDS);
        nonPutOverheadMillionthsHistogramSupplier.get().update(
                getNonPutOverheadMillionths(profile, postCommitOverhead, nonPutOverhead));
    }

    @VisibleForTesting
    static long getNonPutOverhead(TransactionCommitProfile profile, long postCommitOverhead) {
        return profile.totalCommitStageMicros() - profile.keyValueServiceWriteMicros() + postCommitOverhead;
    }

    @VisibleForTesting
    static long getNonPutOverheadMillionths(
            TransactionCommitProfile profile,
            long postCommitOverhead,
            long nonPutOverhead) {
        long totalRelevantTime = profile.totalCommitStageMicros() + postCommitOverhead;
        if (totalRelevantTime == 0) {
            return 0;
        }
        return Math.round(1_000_000. * nonPutOverhead / totalRelevantTime);
    }
}
