/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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
import com.palantir.processors.AutoDelegate;

import feign.Client;
import feign.Request;
import feign.Response;

@AutoDelegate(typeToExtend = Client.class)
public class ScheduledRefreshingClient implements Client {
    private static final Logger log = LoggerFactory.getLogger(ScheduledRefreshingClient.class);

    private static final long DEFAULT_REQUEST_COUNT_BEFORE_REFRESH = 500_000_000L;

    private final Supplier<Client> refreshingSupplier;
    private final long requestCountBeforeRefresh;
    private final AtomicLong counter;

    private volatile Client currentClient;

    @VisibleForTesting
    ScheduledRefreshingClient(Supplier<Client> baseClientSupplier, long requestCountBeforeRefresh) {
        this.refreshingSupplier = baseClientSupplier;
        this.requestCountBeforeRefresh = requestCountBeforeRefresh;
        this.counter = new AtomicLong();
        this.currentClient = refreshingSupplier.get();
    }

    public static Client createRefreshingClient(Supplier<Client> baseClientSupplier) {
        return new ScheduledRefreshingClient(baseClientSupplier, DEFAULT_REQUEST_COUNT_BEFORE_REFRESH);
    }

    @Override
    public Response execute(Request request, Request.Options options) throws IOException {
        return delegate().execute(request, options);
    }

    private Client delegate() {
        long currentCount = counter.incrementAndGet();
        if (currentCount > requestCountBeforeRefresh && counter.compareAndSet(currentCount, 0)) {
            // This is a bit racy, but we ensure only one thread got to do the refresh.
            log.info("Creating a new Feign client, as we believe that {} requests have been made.",
                    SafeArg.of("count", currentCount));
            currentClient = refreshingSupplier.get();
        }
        return currentClient;
    }
}
