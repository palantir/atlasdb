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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.api.expectations.ExpectationsAwareTransaction;
import com.palantir.atlasdb.transaction.api.expectations.ImmutableTransactionViolationFlags;
import com.palantir.atlasdb.transaction.api.expectations.TransactionViolationFlags;
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

    // todo(aalouane) test this class after impl is done
    @Override
    public void run() {
        try {

            Set<TransactionViolationFlags> flagSet = transactions.stream()
                    .map(ExpectationsAwareTransaction::checkAndGetViolations)
                    .collect(ImmutableSet.toImmutableSet());

            updateMetrics(ImmutableTransactionViolationFlags.builder()
                    .ranForTooLong(flagSet.stream().anyMatch(TransactionViolationFlags::ranForTooLong))
                    .readTooMuch(flagSet.stream().anyMatch(TransactionViolationFlags::readTooMuch))
                    .readTooMuchInOneKvsCall(
                            flagSet.stream().anyMatch(TransactionViolationFlags::readTooMuchInOneKvsCall))
                    .queriedKvsTooMuch(flagSet.stream().anyMatch(TransactionViolationFlags::queriedKvsTooMuch))
                    .build());

        } catch (Throwable throwable) {
            log.warn(
                    "Transactional Expectations task failed",
                    SafeArg.of("trackedTransactionsCount", transactions.size()),
                    throwable);
        }
    }

    // todo(aalouane) wire metrics and for each violation type mark violationOccurred-equivalent metric as 1 or 0
    private void updateMetrics(TransactionViolationFlags violationFlags) {}
}
