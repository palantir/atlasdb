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

package com.palantir.atlasdb.timelock;

import com.google.common.util.concurrent.FluentFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.http.RedirectRetryTargeter;
import com.palantir.conjure.java.api.errors.QosException;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.impl.TooManyRequestsException;
import com.palantir.lock.remoting.BlockingTimeoutException;
import java.time.Duration;
import java.util.function.Supplier;

public class ConjureResourceExceptionHandler {
    private final RedirectRetryTargeter redirectRetryTargeter;

    public ConjureResourceExceptionHandler(RedirectRetryTargeter redirectRetryTargeter) {
        this.redirectRetryTargeter = redirectRetryTargeter;
    }

    public <T> ListenableFuture<T> handleExceptions(Supplier<ListenableFuture<T>> supplier) {
        return handleExceptions(Futures.submitAsync(supplier::get, MoreExecutors.directExecutor()));
    }

    private <T> ListenableFuture<T> handleExceptions(ListenableFuture<T> future) {
        return FluentFuture.from(future)
                .catching(
                        BlockingTimeoutException.class,
                        timeout -> {
                            throw QosException.throttle(Duration.ZERO);
                        },
                        MoreExecutors.directExecutor())
                .catching(
                        NotCurrentLeaderException.class,
                        notCurrentLeader -> {
                            throw redirectRetryTargeter
                                    .redirectRequest(notCurrentLeader.getServiceHint())
                                    .<QosException>map(QosException::retryOther)
                                    .orElseGet(QosException::unavailable);
                        },
                        MoreExecutors.directExecutor())
                .catching(
                        TooManyRequestsException.class,
                        tooManyRequests -> {
                            throw QosException.throttle();
                        },
                        MoreExecutors.directExecutor());
    }
}
