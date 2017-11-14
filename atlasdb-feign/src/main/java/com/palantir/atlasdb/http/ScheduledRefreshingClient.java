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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Suppliers;
import com.palantir.processors.AutoDelegate;

import feign.AutoDelegate_Client;
import feign.Client;

@AutoDelegate(typeToExtend = Client.class)
public class ScheduledRefreshingClient implements AutoDelegate_Client {
    private static final Logger log = LoggerFactory.getLogger(ScheduledRefreshingClient.class);
    private static final Duration STANDARD_REFRESH_INTERVAL = Duration.ofDays(1L);

    private final com.google.common.base.Supplier<Client> refreshingSupplier;

    private ScheduledRefreshingClient(Supplier<Client> baseClientSupplier, Duration refreshInterval) {
        refreshingSupplier = Suppliers.memoizeWithExpiration(
                () -> {
                    log.info("Creating a new Feign client, as we believe a day has passed.");
                    return baseClientSupplier.get();
                },
                refreshInterval.toMillis(),
                TimeUnit.MILLISECONDS);
    }

    public static Client createRefreshingClient(Supplier<Client> baseClientSupplier) {
        return new ScheduledRefreshingClient(baseClientSupplier, STANDARD_REFRESH_INTERVAL);
    }

    @Override
    public Client delegate() {
        return refreshingSupplier.get();
    }
}
