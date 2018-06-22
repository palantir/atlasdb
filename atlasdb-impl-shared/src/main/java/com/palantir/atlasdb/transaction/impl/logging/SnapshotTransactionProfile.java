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

import org.immutables.value.Value;

@Value.Immutable
public interface SnapshotTransactionProfile {
    long acquireRowLocksMicros();
    long conflictCheckMicros();
    long writingToSweepQueueMicros();
    long keyValueServiceWriteMicros();
    long getCommitTimestampMicros();
    long punchMicros();
    long readWriteConflictCheckMicros();
    long verifyPreCommitLockCheckMicros();
    long verifyUserPreCommitConditionMicros();
    long putCommitTimestampMicros();

    long totalCommitStageMicros();
    long totalTimeSinceTransactionCreation();

    long commitTimestamp();

    @Value.Lazy
    default long nonPutOverhead() {
        return totalCommitStageMicros() - keyValueServiceWriteMicros();
    }

    /**
     * Returns an estimate of the proportion of time spent outside of the primary key value service write.
     *
     * This exists as a hack because Dropwizard Histograms do not support double values.
     * This is written in this way (doing division first) for precision.
     */
    @Value.Lazy
    default long nonPutOverheadMillionths() {
        return Math.round(1_000_000. * (((double) nonPutOverhead()) / totalCommitStageMicros()));
    }
}
