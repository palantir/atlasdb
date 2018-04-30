/*
 * (c) Copyright 2015 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.cleaner;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;

/**
 * Wrap another Puncher, optimizing the #punch() operation to operate just on a local variable; the
 * underlying "real" punch operation is only invoked asynchronously at a fixed interval, with the
 * latest supplied timestamp as the parameter.
 *
 * @author jweel
 */
public final class AsyncPuncher implements Puncher {
    private static final Logger log = LoggerFactory.getLogger(AsyncPuncher.class);
    private static final long INVALID_TIMESTAMP = -1L;

    public static AsyncPuncher create(Puncher delegate, long interval) {
        AsyncPuncher asyncPuncher = new AsyncPuncher(delegate, interval);
        asyncPuncher.start();
        return asyncPuncher;
    }

    private final ScheduledExecutorService service = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("puncher", true /* daemon */));

    private final Puncher delegate;
    private final long interval;
    private final AtomicLong lastTimestamp = new AtomicLong(INVALID_TIMESTAMP);

    private AsyncPuncher(Puncher delegate, long interval) {
        this.delegate = delegate;
        this.interval = interval;
    }

    private void start() {
        service.scheduleAtFixedRate(() -> {
            long timestamp = lastTimestamp.getAndSet(INVALID_TIMESTAMP);
            if (timestamp != INVALID_TIMESTAMP) {
                delegate.punch(timestamp);
            }
        }, 0, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isInitialized() {
        return delegate.isInitialized();
    }

    @Override
    public void punch(long timestamp) {
        lastTimestamp.set(timestamp);
    }

    @Override
    public Supplier<Long> getTimestampSupplier() {
        return delegate.getTimestampSupplier();
    }

    @Override
    public void shutdown() {
        delegate.shutdown();
        service.shutdownNow();
        boolean shutdown = false;
        try {
            shutdown = service.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted while shutting down the puncher. This shouldn't happen.");
            Thread.currentThread().interrupt();
        }
        if (!shutdown) {
            log.error("Failed to shutdown puncher in a timely manner. The puncher may attempt"
                    + " to access a key value service after the key value service closes. This shouldn't"
                    + " cause any problems, but may result in some scary looking error messages.");
        }
    }
}
