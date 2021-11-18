/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.BlockStrategies;
import com.github.rholder.retry.BlockStrategy;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.WaitStrategies;
import com.github.rholder.retry.WaitStrategy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.transaction.api.TransactionFailedException;
import com.palantir.common.base.Throwables;
import com.palantir.exception.NotInitializedException;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;

public final class TransactionRetryStrategy {
    @SuppressWarnings("ImmutableEnumChecker") // TransactionRetryStrategy is immutable, despite what error-prone thinks
    public enum Strategies {
        LEGACY(createLegacy(BlockStrategies.threadSleepStrategy())),
        EXPONENTIAL(createExponential(BlockStrategies.threadSleepStrategy(), new Random()));

        private final TransactionRetryStrategy strategy;

        Strategies(TransactionRetryStrategy strategy) {
            this.strategy = strategy;
        }

        public TransactionRetryStrategy get() {
            return strategy;
        }
    }

    private static final SafeLogger log = SafeLoggerFactory.get(TransactionRetryStrategy.class);
    private final WaitStrategy waitStrategy;
    private final BlockStrategy blockStrategy;

    private TransactionRetryStrategy(WaitStrategy waitStrategy, BlockStrategy blockStrategy) {
        this.waitStrategy = waitStrategy;
        this.blockStrategy = blockStrategy;
    }

    @SuppressWarnings("rawtypes") // StopStrategy uses raw Attempt
    public <T, E extends Exception> T runWithRetry(IntPredicate shouldStopRetrying, Retryable<T, E> task) throws E {
        UUID runId = UUID.randomUUID();
        Retryer<T> retryer = RetryerBuilder.<T>newBuilder()
                .retryIfException(TransactionRetryStrategy::shouldRetry)
                .withBlockStrategy(blockStrategy)
                .withWaitStrategy(waitStrategy)
                .withStopStrategy(
                        failedAttempt -> shouldStopRetrying.test(Ints.checkedCast(failedAttempt.getAttemptNumber())))
                .withRetryListener(new RetryListener() {
                    @Override
                    public <V> void onRetry(Attempt<V> attempt) {
                        logAttempt(runId, attempt, shouldStopRetrying);
                    }
                })
                .build();
        try {
            return retryer.call(task::run);
        } catch (RetryException e) {
            Throwable wrapped = Throwables.rewrap(
                    String.format("Failing after %d tries.", e.getNumberOfFailedAttempts()),
                    e.getLastFailedAttempt().getExceptionCause());
            throw throwTaskException(wrapped);
        } catch (ExecutionException e) {
            throw throwTaskException(e.getCause());
        }
    }

    private <T> void logAttempt(UUID runId, Attempt<T> attempt, IntPredicate shouldStopRetrying) {
        int failureCount = Ints.checkedCast(attempt.getAttemptNumber()) - 1;
        if (attempt.hasResult()) {
            if (failureCount > 0) {
                log.info(
                        "[{}] Successfully completed transaction after {} retries.",
                        SafeArg.of("runId", runId),
                        SafeArg.of("failureCount", failureCount));
            }
        } else {
            Throwable thrown = attempt.getExceptionCause();
            if (thrown instanceof TransactionFailedException) {
                TransactionFailedException exception = (TransactionFailedException) thrown;
                if (!exception.canTransactionBeRetried()) {
                    log.warn(
                            "[{}] Non-retriable exception while processing transaction.",
                            SafeArg.of("runId", runId),
                            SafeArg.of("failureCount", failureCount));
                } else if (shouldStopRetrying.test(failureCount)) {
                    log.warn(
                            "[{}] Failing after {} tries.",
                            SafeArg.of("runId", runId),
                            SafeArg.of("failureCount", failureCount),
                            exception);
                } else if (failureCount > 2) {
                    log.info(
                            "[{}] Retrying transaction after {} failure(s).",
                            SafeArg.of("runId", runId),
                            SafeArg.of("failureCount", failureCount),
                            thrown);
                }
            } else if (thrown instanceof NotInitializedException) {
                log.info("TransactionManager is not initialized. Aborting transaction with runTaskWithRetry", thrown);
            } else if (thrown instanceof RuntimeException) {
                log.debug("[{}] RuntimeException while processing transaction.", SafeArg.of("runId", runId), thrown);
            }
        }
    }

    /**
     * This is overly unchecked in order to handle the transaction tasks's potential for checked exceptions.
     */
    private static <E extends Exception> RuntimeException throwTaskException(Throwable thrown) throws E {
        if (thrown instanceof Error) {
            throw (Error) thrown;
        } else if (thrown instanceof Exception) {
            throw (E) thrown;
        } else {
            throw new RuntimeException(thrown);
        }
    }

    @FunctionalInterface
    public interface Retryable<T, E extends Exception> {
        T run() throws E;
    }

    @SuppressWarnings("rawtypes") // WaitStrategy uses raw Attempt
    private static WaitStrategy randomize(Random random, WaitStrategy strategy) {
        return attempt -> random.nextInt(Ints.checkedCast(strategy.computeSleepTime(attempt)));
    }

    private static boolean shouldRetry(Throwable e) {
        return e instanceof TransactionFailedException && ((TransactionFailedException) e).canTransactionBeRetried();
    }

    @JsonCreator
    static TransactionRetryStrategy of(Strategies strategy) {
        return strategy.strategy;
    }

    @VisibleForTesting
    static TransactionRetryStrategy createLegacy(BlockStrategy blockStrategy) {
        return new TransactionRetryStrategy(WaitStrategies.noWait(), blockStrategy);
    }

    @VisibleForTesting
    static TransactionRetryStrategy createExponential(BlockStrategy blockStrategy, Random random) {
        return new TransactionRetryStrategy(
                randomize(random, WaitStrategies.exponentialWait(100, 1, TimeUnit.MINUTES)), blockStrategy);
    }
}
