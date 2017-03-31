/*
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timelock.atomix;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.atomix.copycat.session.ClosedSessionException;

public final class AtomixRetryer {
    private static final Logger log = LoggerFactory.getLogger(AtomixRetryer.class);

    public static final int RETRY_ATTEMPTS = 3;
    private static final Retryer<Object> RETRYER = RetryerBuilder.newBuilder()
            .retryIfException(AtomixRetryer::canBeRetried)
            .withRetryListener(new RetryListener() {
                @Override
                public <V> void onRetry(Attempt<V> attempt) {
                    if (attempt.hasException() && canBeRetried(attempt.getExceptionCause())) {
                        log.warn("Encountered a retriable exception [{}] in an Atomix operation (attempt {}/{}). ",
                                attempt.getExceptionCause(),
                                attempt.getAttemptNumber(),
                                RETRY_ATTEMPTS);
                    }
                }
            })
            .withStopStrategy(StopStrategies.stopAfterAttempt(RETRY_ATTEMPTS))
            .build();

    private AtomixRetryer() {
        // utility
    }

    @SuppressWarnings("unchecked") // We only want to create 1 retryer; this is safe given the type of supplier.
    public static <T> T getWithRetry(Supplier<CompletableFuture<T>> supplier) {
        try {
            return (T) RETRYER.call(() -> Futures.getUnchecked(supplier.get()));
        } catch (RetryException e) {
            throw Throwables.propagate(e);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private static boolean canBeRetried(Throwable throwable) {
        return throwable instanceof UncheckedExecutionException
                && throwable.getCause() instanceof ClosedSessionException;
    }
}
