/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.factory.startup;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.factory.TransactionManagerConsistencyResult;
import com.palantir.atlasdb.factory.TransactionManagerConsistencyResults;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.consistency.TransactionManagerConsistencyCheck;
import com.palantir.common.base.Throwables;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Comparator;
import java.util.List;

/**
 * Executes multiple {@link TransactionManagerConsistencyCheck}s in sequence, aggregating the
 * {@link TransactionManagerConsistencyResult}s they return. Aggregation is done by taking the result with the highest
 * severity score.
 *
 * The aggregated result is then processed: we throw an {@link AssertionError} on a TERMINAL result and throw a
 * {@link NotInitializedException} on an INDETERMINATE result.
 */
public final class ConsistencyCheckRunner extends Callback<TransactionManager> {

    private static final SafeLogger log = SafeLoggerFactory.get(ConsistencyCheckRunner.class);

    private static final RuntimeException UNKNOWN = new SafeRuntimeException("unknown");

    private final List<TransactionManagerConsistencyCheck> consistencyChecks;

    @VisibleForTesting
    ConsistencyCheckRunner(List<TransactionManagerConsistencyCheck> consistencyChecks) {
        this.consistencyChecks = consistencyChecks;
    }

    public static ConsistencyCheckRunner create(TransactionManagerConsistencyCheck check) {
        return new ConsistencyCheckRunner(ImmutableList.of(check));
    }

    @Override
    public void init(TransactionManager resource) {
        TransactionManagerConsistencyResult consistencyResult = checkAndAggregateResults(resource);
        processAggregatedResult(consistencyResult);
    }

    private TransactionManagerConsistencyResult checkAndAggregateResults(TransactionManager resource) {
        return consistencyChecks.stream()
                .map(check -> check.apply(resource))
                .max(Comparator.comparingLong(
                        result -> result.consistencyState().severity()))
                .orElse(TransactionManagerConsistencyResults.CONSISTENT_RESULT);
    }

    private void processAggregatedResult(TransactionManagerConsistencyResult consistencyResult) {
        switch (consistencyResult.consistencyState()) {
            case TERMINAL:
                // Errors get bubbled up to the top level
                throw new AssertionError(
                        "AtlasDB found in an unexpected state!",
                        consistencyResult.reasonForInconsistency().orElse(UNKNOWN));
            case INDETERMINATE:
                throw new NotInitializedException("ConsistencyCheckRunner");
            case CONSISTENT:
                log.info("Cluster appears consistent.");
                break;
            default:
                throw new IllegalStateException("Unexpected consistency state " + consistencyResult.consistencyState());
        }
    }

    @Override
    public void cleanup(TransactionManager _resource, Throwable initThrowable) {
        // Propagate errors, but there's no need to do cleanup as each task is responsible for that,
        // and this class assumes the tasks are independent.
        if (!(initThrowable instanceof NotInitializedException)) {
            throw Throwables.rewrapAndThrowUncheckedException(initThrowable);
        }
    }
}
