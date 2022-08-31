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

import com.palantir.atlasdb.transaction.service.TransactionStatus;
import com.palantir.atlasdb.transaction.service.TransactionStatuses;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import java.util.function.Function;

public final class TransactionStatusUtils {
    private TransactionStatusUtils() {}

    public static TransactionStatus fromTimestamp(long timestamp) {
        if (timestamp == TransactionConstants.FAILED_COMMIT_TS) {
            return TransactionConstants.ABORTED;
        }
        return TransactionStatuses.committed(timestamp);
    }

    public static long getCommitTimestampOrThrow(TransactionStatus status) {
        return TransactionStatuses.caseOf(status)
                .committed(Function.identity())
                .aborted_(TransactionConstants.FAILED_COMMIT_TS)
                .otherwise(() -> {
                    throw new SafeIllegalStateException("Illegal transaction status", SafeArg.of("status", status));
                });
    }

    public static Optional<Long> maybeGetCommitTs(long startTs, TransactionStatus status) {
        return TransactionStatuses.caseOf(status)
                .committed(Function.identity())
                .aborted_(TransactionConstants.FAILED_COMMIT_TS)
                .unknown(() -> startTs)
                .otherwiseEmpty();
    }
}
