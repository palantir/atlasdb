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

package com.palantir.atlasdb.transaction.impl;

import com.palantir.atlasdb.transaction.api.expectations.ExpectationsConfig;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionViolationFlags;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionViolationFlags;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;

public final class ExpectationsViolationsChecker {
    private static final SafeLogger log = SafeLoggerFactory.get(ExpectationsViolationsChecker.class);

    private ExpectationsViolationsChecker() {}

    public static TransactionViolationFlags checkAndGetViolations(ExpectationsAwareTransaction transaction) {
        ExpectationsConfig configSnapshot = transaction.expectationsConfig();
        long ageSoFarMillis = transaction.getAgeMillis();
        TransactionReadInfo readInfoSnapshot = transaction.getReadInfo();
        return ImmutableTransactionViolationFlags.builder()
                .ranForTooLong(checkForRanForTooLong(configSnapshot, ageSoFarMillis))
                .readTooMuch(checkForReadTooMuch(configSnapshot, readInfoSnapshot))
                .readTooMuchInOneKvsCall(checkForReadTooMuchInOneKvsCall(configSnapshot, readInfoSnapshot))
                .queriedKvsTooMuch(checkForQueriedKvsTooMuch(configSnapshot, readInfoSnapshot))
                .build();
    }

    private static boolean checkForRanForTooLong(ExpectationsConfig config, long ageSoFarMillis) {
        if (ageSoFarMillis <= config.transactionAgeMillisLimit()) {
            return false;
        }

        log.warn(
                "Transaction ran for too long",
                SafeArg.of("name", config.transactionDisplayName()),
                SafeArg.of("ageMillis", ageSoFarMillis),
                SafeArg.of("ageMillisLimit", config.transactionAgeMillisLimit()));

        return false;
    }

    private static boolean checkForReadTooMuch(ExpectationsConfig config, TransactionReadInfo readInfo) {
        long bytesRead = readInfo.bytesRead();

        if (bytesRead <= config.bytesReadLimit()) {
            return false;
        }

        log.warn(
                "Transaction read too much",
                SafeArg.of("name", config.transactionDisplayName()),
                SafeArg.of("bytesRead", bytesRead),
                SafeArg.of("bytesReadLimit", config.bytesReadLimit()));

        return true;
    }

    private static boolean checkForQueriedKvsTooMuch(ExpectationsConfig config, TransactionReadInfo readInfo) {
        long kvsCallCount = readInfo.kvsCalls();

        if (kvsCallCount <= config.kvsReadCallCountLimit()) {
            return false;
        }

        log.warn(
                "Transaction made too many atlas kvs calls",
                SafeArg.of("name", config.transactionName()),
                SafeArg.of("atlasKvsCalls", kvsCallCount),
                SafeArg.of("atlasKvsCallsLimit", config.kvsReadCallCountLimit()));

        return true;
    }

    private static boolean checkForReadTooMuchInOneKvsCall(ExpectationsConfig config, TransactionReadInfo readInfo) {
        return readInfo.maximumBytesKvsCallInfo()
                .map(kvsCallInfo -> {
                    long bytesRead = kvsCallInfo.bytesRead();

                    if (bytesRead <= config.bytesReadInOneKvsCallLimit()) {
                        return false;
                    }

                    log.warn(
                            "Transaction read too much in one atlas kvs call",
                            SafeArg.of("name", config.transactionName()),
                            SafeArg.of("atlasKvsCallBytesRead", bytesRead),
                            SafeArg.of("atlasKvsCallBytesReadLimit", config.bytesReadInOneKvsCallLimit()));

                    return true;
                })
                .orElse(false);
    }
}
