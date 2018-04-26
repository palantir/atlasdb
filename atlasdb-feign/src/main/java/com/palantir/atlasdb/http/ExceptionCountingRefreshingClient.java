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

package com.palantir.atlasdb.http;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.logsafe.SafeArg;

import feign.Client;
import feign.Request;
import feign.Response;

public class ExceptionCountingRefreshingClient implements Client {
    private static final Logger log = LoggerFactory.getLogger(ExceptionCountingRefreshingClient.class);

    private static final long DEFAULT_EXCEPTION_COUNT_BEFORE_REFRESH = 10_000L;

    private final Supplier<Client> refreshingSupplier;
    private final long exceptionCountBeforeRefresh;
    private final AtomicLong counter;

    private volatile Client currentClient;

    @VisibleForTesting
    ExceptionCountingRefreshingClient(Supplier<Client> baseClientSupplier,
            long exceptionCountBeforeRefresh) {
        this.refreshingSupplier = baseClientSupplier;
        this.exceptionCountBeforeRefresh = exceptionCountBeforeRefresh;
        this.counter = new AtomicLong();
        this.currentClient = refreshingSupplier.get();
    }

    public static Client createRefreshingClient(Supplier<Client> baseClientSupplier) {
        return new ExceptionCountingRefreshingClient(baseClientSupplier, DEFAULT_EXCEPTION_COUNT_BEFORE_REFRESH);
    }

    @Override
    public Response execute(Request request, Request.Options options) throws IOException {
        try {
            Response response = delegate().execute(request, options);
            counter.set(0);
            return response;
        } catch (Exception e) {
            counter.incrementAndGet();
            throw e;
        }
    }

    private Client delegate() {
        long currentCount = counter.get();
        if (currentCount >= exceptionCountBeforeRefresh && counter.compareAndSet(currentCount, 0)) {
            log.info("Creating a new Feign client, as {} exceptions have been thrown in a row by old client",
                    SafeArg.of("count", currentCount));
            try {
                currentClient = refreshingSupplier.get();
            } catch (RuntimeException e) {
                log.warn("An error occurred whilst trying to re-create an OkHttpClient, because {} exceptions have been thrown in a row by old client."
                                + " Continuing operation with the old client...",
                        SafeArg.of("count", currentCount),
                        e);
            }
        }
        return currentClient;
    }
}
