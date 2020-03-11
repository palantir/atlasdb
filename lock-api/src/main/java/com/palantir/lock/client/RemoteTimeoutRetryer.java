/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import java.net.SocketTimeoutException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;

class RemoteTimeoutRetryer {
    private final Clock clock;

    @VisibleForTesting
    RemoteTimeoutRetryer(Clock clock) {
        this.clock = clock;
    }

    static RemoteTimeoutRetryer createDefault() {
        return new RemoteTimeoutRetryer(Clock.systemUTC());
    }

    <S, T> T attemptUntilTimeLimitOrException(
            S request,
            Function<S, Duration> durationExtractor,
            BiFunction<S, Duration, S> durationLimiter,
            Function<S, T> query,
            Predicate<T> successfulResponseEvaluator,
            T defaultResponse) {
        Instant now = clock.instant();
        Instant deadline = now.plus(durationExtractor.apply(request));

        while (now.isBefore(deadline)) {
            Duration remainingTime = Duration.between(now, deadline);
            S durationLimitedInput = durationLimiter.apply(request, remainingTime);

            try {
                T response = query.apply(durationLimitedInput);
                if (successfulResponseEvaluator.test(response)) {
                    return response;
                }
            } catch (Exception e) {
                if (!isPlausiblyTimeout(e) || clock.instant().isAfter(deadline)) {
                    throw e;
                }
            }
            now = clock.instant();
        }
        return defaultResponse;
    }

    private static boolean isPlausiblyTimeout(Throwable t) {
        return t instanceof SocketTimeoutException || (t.getCause() != null && isPlausiblyTimeout(t.getCause()));
    }
}
