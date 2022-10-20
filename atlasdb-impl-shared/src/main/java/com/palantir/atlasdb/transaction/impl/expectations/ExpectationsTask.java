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

package com.palantir.atlasdb.transaction.impl.expectations;

import com.palantir.atlasdb.transaction.api.expectations.ExpectationsAwareTransaction;
import com.palantir.atlasdb.transaction.api.expectations.ExpectationsConfig;
import com.palantir.atlasdb.transaction.api.expectations.KvsCallReadInfo;
import com.palantir.atlasdb.transaction.api.expectations.TransactionReadInfo;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Set;

public final class ExpectationsTask implements Runnable {
    private static final SafeLogger log = SafeLoggerFactory.get(ExpectationsTask.class);
    private final Set<ExpectationsAwareTransaction> transactions;

    public ExpectationsTask(Set<ExpectationsAwareTransaction> transactions) {
        this.transactions = transactions;
    }

    @Override
    public void run() {
        try {
            transactions.forEach(this::checkExpectations);
            updateMetrics();
        } catch (Throwable throwable) {
            log.warn(
                    "Transactional Expectations task failed",
                    SafeArg.of("trackedTransactionsCount", transactions.size()),
                    throwable);
        }
    }

    private void checkExpectations(ExpectationsAwareTransaction transaction) {
        ExpectationsConfig config = transaction.expectationsConfig();
        long ageMillis = transaction.getAgeMillis();
        TransactionReadInfo readInfo = transaction.getReadInfo();

        checkAge(ageMillis, config);
        checkBytesRead(readInfo.bytesRead(), config);
        checkKvsReadCalls(readInfo.kvsCalls(), config);
        readInfo.maximumBytesKvsCallInfo()
                .ifPresent(kvsCallReadInfo -> checkMaximumBytesReadPerKvsCall(kvsCallReadInfo, config));
    }

    private void updateMetrics() {}

    private void checkAge(long ageMillis, ExpectationsConfig config) {
        if (ageMillis > config.transactionAgeMillisLimit()) {
            log.warn(
                    "Transaction running for too long",
                    SafeArg.of("transactionAgeMillis", ageMillis),
                    SafeArg.of("transactionAgeMillisLimit", config.transactionAgeMillisLimit()),
                    SafeArg.of("transactionName", config.transactionDisplayName()));
        }
    }

    private void checkBytesRead(long bytesRead, ExpectationsConfig config) {
        if (bytesRead > config.bytesReadLimit()) {
            log.warn(
                    "Transaction reading too much",
                    SafeArg.of("bytesRead", bytesRead),
                    SafeArg.of("bytesReadLimit", config.bytesReadLimit()),
                    SafeArg.of("transactionName", config.transactionDisplayName()));
        }
    }

    private void checkMaximumBytesReadPerKvsCall(KvsCallReadInfo maximumBytesKvsCallInfo, ExpectationsConfig config) {
        if (maximumBytesKvsCallInfo.bytesRead() > config.bytesReadInOneKvsCallLimit()) {
            log.warn(
                    "Transaction reading too much in a single kvs call",
                    SafeArg.of("kvsCallBytesRead", maximumBytesKvsCallInfo.bytesRead()),
                    SafeArg.of("kvsCallBytesReadLimit", config.bytesReadInOneKvsCallLimit()),
                    SafeArg.of("kvsMethodName", maximumBytesKvsCallInfo.methodName()),
                    SafeArg.of("transactionName", config.transactionDisplayName()));
        }
    }

    private void checkKvsReadCalls(long calls, ExpectationsConfig config) {
        if (calls > config.kvsReadCallCountLimit()) {
            log.warn(
                    "Transaction making too many kvs calls",
                    SafeArg.of("kvsCalls", calls),
                    SafeArg.of("kvsReadCallCountLimit", config.kvsReadCallCountLimit()),
                    SafeArg.of("transactionName", config.transactionDisplayName()));
        }
    }
}
