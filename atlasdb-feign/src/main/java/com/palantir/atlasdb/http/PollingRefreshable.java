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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.remoting3.ext.refresh.Refreshable;

public final class PollingRefreshable<T> implements AutoCloseable {
    // TODO (jkong): Should this be configurable?
    private static final Duration POLL_INTERVAL = Duration.ofSeconds(5L);

    private final Supplier<T> supplier;
    private final ScheduledExecutorService poller;

    private final Refreshable<T> refreshable;

    private volatile T lastSeenValue;

    private PollingRefreshable(Supplier<T> supplier, ScheduledExecutorService poller) {
        this.supplier = supplier;
        this.poller = poller;

        T initialValue = supplier.get();
        refreshable = Refreshable.of(initialValue);
        lastSeenValue = initialValue;
    }

    public static <T> PollingRefreshable<T> create(Supplier<T> supplier) {
        PollingRefreshable<T> pollingRefreshable = new PollingRefreshable<>(
                supplier,
                Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("polling-refreshable", true)));
        pollingRefreshable.scheduleUpdates();
        return pollingRefreshable;
    }

    public Refreshable<T> getRefreshable() {
        return refreshable;
    }

    private void scheduleUpdates() {
        poller.scheduleWithFixedDelay(() -> {
            T value = supplier.get();
            if (!value.equals(lastSeenValue)) {
                lastSeenValue = value;
                refreshable.set(lastSeenValue);
            }
        }, POLL_INTERVAL.getSeconds(), POLL_INTERVAL.getSeconds(), TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
        poller.shutdown();
    }
}
