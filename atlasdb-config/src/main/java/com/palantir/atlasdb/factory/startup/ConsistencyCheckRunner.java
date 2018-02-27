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

package com.palantir.atlasdb.factory.startup;

import java.util.Comparator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.palantir.async.initializer.Callback;
import com.palantir.atlasdb.factory.TransactionManagerConsistencyResult;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.atlasdb.transaction.impl.consistency.TransactionManagerConsistencyCheck;
import com.palantir.exception.NotInitializedException;

public final class ConsistencyCheckRunner extends Callback<TransactionManager> {

    private static final Logger log = LoggerFactory.getLogger(ConsistencyCheckRunner.class);

    private static final RuntimeException UNKNOWN = new RuntimeException("unknown");

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
                .max(Comparator.comparingLong(result -> result.consistencyState().severity()))
                .orElse(TransactionManagerConsistencyResult.CONSISTENT_RESULT);
    }

    private void processAggregatedResult(TransactionManagerConsistencyResult consistencyResult) {
        switch (consistencyResult.consistencyState()) {
            case TERMINAL:
                // Errors get bubbled up to the top level
                throw new AssertionError("AtlasDB found in an unexpected state!",
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
    public void cleanup(TransactionManager resource, Exception initException) {
        // No op; consistency checks shouldn't use their own resources / should clean up after themselves.
    }
}
