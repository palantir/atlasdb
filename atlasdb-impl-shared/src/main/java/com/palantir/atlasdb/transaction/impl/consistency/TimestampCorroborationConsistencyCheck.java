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
package com.palantir.atlasdb.transaction.impl.consistency;

import com.palantir.atlasdb.factory.ImmutableTransactionManagerConsistencyResult;
import com.palantir.atlasdb.factory.TransactionManagerConsistencyResult;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.logsafe.SafeArg;
import java.util.function.ToLongFunction;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compares a source of fresh timestamps against a conservative lower bound that should always be strictly lower than
 * a fresh timestamp, reporting inconsistency if a fresh timestamp is actually lower than this lower bound.
 *
 * In typical usage, a recent timestamp from the puncher store is used as a conservative bound, and the source of fresh
 * timestamps could be a TimeLock server or other timestamp service.
 */
@Value.Immutable
public abstract class TimestampCorroborationConsistencyCheck implements TransactionManagerConsistencyCheck {
    private static final Logger log = LoggerFactory.getLogger(TimestampCorroborationConsistencyCheck.class);

    protected abstract ToLongFunction<TransactionManager> conservativeBound();

    protected abstract ToLongFunction<TransactionManager> freshTimestampSource();

    @Override
    public TransactionManagerConsistencyResult apply(TransactionManager transactionManager) {
        // The ordering is important, because if we get a timestamp first, we may have a false positive if we have
        // a long GC between grabbing the fresh timestamp and the lower bound (e.g. if someone punches in between).
        long lowerBound;
        long freshTimestamp;

        try {
            lowerBound = conservativeBound().applyAsLong(transactionManager);
        } catch (Exception e) {
            log.warn(
                    "Could not obtain a lower bound on timestamps, so we don't know if our transaction manager"
                            + " is consistent.",
                    e);
            return indeterminateResultForException(e);
        }

        try {
            freshTimestamp = freshTimestampSource().applyAsLong(transactionManager);
        } catch (Exception e) {
            log.warn(
                    "Could not obtain a fresh timestamp, so we don't know if our transaction manager is"
                            + " consistent.",
                    e);
            return indeterminateResultForException(e);
        }
        if (freshTimestamp <= lowerBound) {
            log.error(
                    "Your AtlasDB client believes that a strict lower bound for the timestamp was {} (typically by"
                            + " reading the unreadable timestamp), but that's newer than a fresh timestamp of {}, which"
                            + " implies clocks went back. If using TimeLock, this could be because timestamp bounds"
                            + " were not migrated properly - which can happen if you've moved TimeLock Server without"
                            + " moving its persistent state. For safety, AtlasDB will refuse to start.",
                    SafeArg.of("timestampLowerBound", lowerBound),
                    SafeArg.of("freshTimestamp", freshTimestamp));
            return ImmutableTransactionManagerConsistencyResult.builder()
                    .consistencyState(TransactionManagerConsistencyResult.ConsistencyState.TERMINAL)
                    .reasonForInconsistency(clocksWentBackwards(lowerBound, freshTimestamp))
                    .build();
        }
        log.info(
                "Passed timestamp corroboration consistency check; expected a strict lower bound of {}, which was"
                        + " lower than a fresh timestamp of {}.",
                SafeArg.of("timestampLowerBound", lowerBound),
                SafeArg.of("freshTimestamp", freshTimestamp));
        return ImmutableTransactionManagerConsistencyResult.builder()
                .consistencyState(TransactionManagerConsistencyResult.ConsistencyState.CONSISTENT)
                .build();
    }

    private TransactionManagerConsistencyResult indeterminateResultForException(Exception ex) {
        return ImmutableTransactionManagerConsistencyResult.builder()
                .consistencyState(TransactionManagerConsistencyResult.ConsistencyState.INDETERMINATE)
                .reasonForInconsistency(ex)
                .build();
    }

    private AssertionError clocksWentBackwards(long lowerBound, long freshTimestamp) {
        String errorMessage = "Expected timestamp to be greater than %s, but a fresh timestamp was %s!";
        return new AssertionError(String.format(errorMessage, lowerBound, freshTimestamp));
    }
}
