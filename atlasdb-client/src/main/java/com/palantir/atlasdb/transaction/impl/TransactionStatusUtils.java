/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatus.Aborted;
import com.palantir.atlasdb.transaction.service.TransactionStatus.Committed;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import java.util.function.Function;

public final class TransactionStatusUtils {

    private TransactionStatusUtils() {}

    public static TransactionStatus fromTimestamp(long timestamp) {
        if (timestamp == TransactionConstants.FAILED_COMMIT_TS) {
            return TransactionStatus.aborted();
        }
        return TransactionStatus.committed(timestamp);
    }

    /**
     * This helper is only meant to be used for transactions with schema < 4. For schemas >= 4,
     * use {@link #getCommitTsFromStatus(long, TransactionStatus, Function)}
     * */
    public static long getCommitTimestampIfKnown(TransactionStatus status) {
        if (status instanceof Committed) {
            Committed committed = (Committed) status;
            return committed.commitTimestamp();
        } else if (status instanceof Aborted) {
            return TransactionConstants.FAILED_COMMIT_TS;
        } else {
            throw new SafeIllegalStateException("Illegal transaction status", SafeArg.of("status", status));
        }
    }

    /**
     * This helper is only meant to be used for transactions with schema < 4. For schemas >= 4,
     * use {@link #getCommitTsFromStatus(long, TransactionStatus, Function)}
     * */
    public static Optional<Long> maybeGetCommitTs(TransactionStatus status) {
        if (status instanceof Committed) {
            Committed committed = (Committed) status;
            return Optional.of(committed.commitTimestamp());
        } else if (status instanceof Aborted) {
            return Optional.of(TransactionConstants.FAILED_COMMIT_TS);
        } else {
            return Optional.empty();
        }
    }

    public static Long getCommitTsFromStatus(
            long startTs, TransactionStatus status, Function<Long, Boolean> abortedCheck) {
        if (status instanceof TransactionStatus.Unknown) {
            return getCommitTsForConcludedTransaction(startTs, abortedCheck);
        } else {
            return TransactionStatusUtils.maybeGetCommitTs(status).orElse(null);
        }
    }

    public static long getCommitTsForConcludedTransaction(long startTs, Function<Long, Boolean> isAborted) {
        return isAborted.apply(startTs)
                ? TransactionConstants.FAILED_COMMIT_TS
                : getCommitTsForNonAbortedUnknownTransaction(startTs);
    }

    @VisibleForTesting
    static long getCommitTsForNonAbortedUnknownTransaction(long startTs) {
        return startTs;
    }
}
