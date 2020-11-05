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

package com.palantir.atlasdb.timelock.batch;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.timelock.AsyncTimelockService;
import com.palantir.atlasdb.timelock.ConjureResourceExceptionHandler;
import com.palantir.atlasdb.timelock.api.NamespacedLeaderTime;
import com.palantir.atlasdb.timelock.batch.api.UndertowCrossClientBatchedConjureTimelockService;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class CrossClientBatchedConjureTimeLockResource implements UndertowCrossClientBatchedConjureTimelockService {
    private final ConjureResourceExceptionHandler exceptionHandler;
    private final Function<String, AsyncTimelockService> timelockServices;

    private CrossClientBatchedConjureTimeLockResource(
            ConjureResourceExceptionHandler exceptionHandler, Function<String, AsyncTimelockService> timelockServices) {
        this.exceptionHandler = exceptionHandler;
        this.timelockServices = timelockServices;
    }

    @Override
    public ListenableFuture<List<NamespacedLeaderTime>> leaderTimes(AuthHeader authHeader, List<String> namespaces) {
        List<ListenableFuture<NamespacedLeaderTime>> futures =
                namespaces.stream().map(namespace -> {
                    ListenableFuture<LeaderTime> leaderTimeListenableFuture = forNamespace(namespace).leaderTime();
                    return Futures.transform(leaderTimeListenableFuture,
                            leaderTime -> NamespacedLeaderTime.of(namespace,
                            leaderTime),
                            MoreExecutors.directExecutor());
                }).collect(Collectors.toList());

        // todo(snanda) failing the entire batch right now
        return handleExceptions(() -> Futures.allAsList(futures));
    }

    private AsyncTimelockService forNamespace(String namespace) {
        return timelockServices.apply(namespace);
    }

    private <T> ListenableFuture<T> handleExceptions(Supplier<ListenableFuture<T>> supplier) {
        return exceptionHandler.handleExceptions(supplier);
    }
}
