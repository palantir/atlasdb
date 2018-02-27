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

package com.palantir.atlasdb.transaction.impl.consistency;

import java.util.function.ToLongFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.factory.ImmutableTransactionManagerConsistencyResult;
import com.palantir.atlasdb.factory.TransactionManagerConsistencyResult;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.logsafe.SafeArg;

public class TimestampCorroborationConsistencyCheck implements TransactionManagerConsistencyCheck {
    private static final Logger log = LoggerFactory.getLogger(TimestampCorroborationConsistencyCheck.class);

    private final ToLongFunction<TransactionManager> conservativeBound;
    private final ToLongFunction<TransactionManager> freshTimestampSource;

    public TimestampCorroborationConsistencyCheck(
            ToLongFunction<TransactionManager> conservativeBound,
            ToLongFunction<TransactionManager> freshTimestampSource) {
        this.conservativeBound = conservativeBound;
        this.freshTimestampSource = freshTimestampSource;
    }


    @Override
    public TransactionManagerConsistencyResult apply(TransactionManager transactionManager) {
        // The ordering is important, because if we get a timestamp first, we may have a false positive if we have
        // a long GC between grabbing the fresh timestamp and the lower bound (e.g. if someone punches in between).
        long lowerBound;
        long freshTimestamp;

        try {
            lowerBound = conservativeBound.applyAsLong(transactionManager);
        } catch (Exception e) {
            log.warn("Could not obtain a lower bound on timestamps, so we don't know if our transaction manager"
                    + " is consistent");
            return indeterminateResultForException(e);
        }

        try {
            freshTimestamp = freshTimestampSource.applyAsLong(transactionManager);
        } catch (Exception e) {
            log.warn("Could not obtain a fresh timestamp, so we don't know if our transaction manager is"
                    + " consistent.");
            return indeterminateResultForException(e);
        }

        if (freshTimestamp < lowerBound) {
            log.error("Your AtlasDB client believes that the unreadable timestamp was {}, but that's newer than a"
                    + " fresh timestamp of {}, which implies clocks went back. If using TimeLock, this could be because"
                    + " timestamp bounds were not migrated properly - which can happen if you've moved TimeLock"
                    + " Server without moving its persistent state. For safety, AtlasDB will refuse to start.",
                    SafeArg.of("unreadableTimestamp", lowerBound),
                    SafeArg.of("freshTimestamp", freshTimestamp));
            return ImmutableTransactionManagerConsistencyResult.builder()
                    .consistencyState(TransactionManagerConsistencyResult.ConsistencyState.TERMINAL)
                    .build();
        }
        log.info("Passed timestamp corroboration consistency check; expected a lower bound of {}, which was"
                        + " lower than a fresh timestamp of {}.",
                SafeArg.of("timestampLowerBound", lowerBound),
                SafeArg.of("freshTimestamp", freshTimestamp));
        return ImmutableTransactionManagerConsistencyResult.builder()
                .consistencyState(TransactionManagerConsistencyResult.ConsistencyState.CONSISTENT)
                .build();
    }

    private TransactionManagerConsistencyResult indeterminateResultForException(Exception e) {
        return ImmutableTransactionManagerConsistencyResult.builder()
                .consistencyState(TransactionManagerConsistencyResult.ConsistencyState.INDETERMINATE)
                .reasonForInconsistency(e)
                .build();
    }
}
