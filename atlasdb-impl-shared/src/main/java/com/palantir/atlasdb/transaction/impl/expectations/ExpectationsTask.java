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
import com.palantir.atlasdb.transaction.api.expectations.ExpectationsViolation;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Set;
import java.util.stream.Collectors;

public final class ExpectationsTask implements Runnable {
    private static final SafeLogger log = SafeLoggerFactory.get(ExpectationsTask.class);
    private final Set<ExpectationsAwareTransaction> transactions;

    public ExpectationsTask(Set<ExpectationsAwareTransaction> transactions) {
        this.transactions = transactions;
    }

    @Override
    public void run() {
        try {
            Set<ExpectationsViolation> violations = transactions.stream()
                    .flatMap(transaction -> transaction.checkAndGetViolations().stream())
                    .collect(Collectors.toSet());
            updateMetrics(violations);
        } catch (Throwable throwable) {
            log.warn(
                    "Transactional Expectations task failed",
                    SafeArg.of("trackedTransactionsCount", transactions.size()),
                    throwable);
        }
    }

    // todo(aalouane) wire metrics and for each violation type mark violationOccurred-equivalent metric as 1 or 0
    private void updateMetrics(Set<ExpectationsViolation> violations) {}
}
