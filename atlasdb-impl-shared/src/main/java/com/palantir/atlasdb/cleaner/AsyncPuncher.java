/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.cleaner;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.SafeArg;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrap another Puncher, optimizing the #punch() operation to operate just on a local variable; the
 * underlying "real" punch operation is only invoked asynchronously at a fixed interval, with the
 * latest supplied timestamp as the parameter.
 *
 * @author jweel
 */
public final class AsyncPuncher implements Puncher {
    private static final Logger log = LoggerFactory.getLogger(AsyncPuncher.class);

    @VisibleForTesting
    static final long INVALID_TIMESTAMP = -1L;

    public static AsyncPuncher create(
            Puncher delegate,
            long interval,
            LongSupplier freshTimestampSupplier) {
        AsyncPuncher asyncPuncher = new AsyncPuncher(delegate, interval, freshTimestampSupplier);
        asyncPuncher.start();
        return asyncPuncher;
    }

    private final ScheduledExecutorService service = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("puncher", true /* daemon */));

    private final Puncher delegate;
    private final long interval;
    private final AtomicLong lastTimestamp;
    private final LongSupplier freshTimestampSource;

    private AsyncPuncher(Puncher delegate, long interval, LongSupplier freshTimestampSource) {
        this.delegate = delegate;
        this.interval = interval;
        this.lastTimestamp = new AtomicLong(INVALID_TIMESTAMP);
        this.freshTimestampSource = freshTimestampSource;
    }

    private void start() {
        service.scheduleAtFixedRate(this::punchWithRollback, 0, interval, TimeUnit.MILLISECONDS);
    }

    private void punchWithRollback() {
        long timestampToPunch = lastTimestamp.getAndSet(INVALID_TIMESTAMP);
        if (timestampToPunch == INVALID_TIMESTAMP) {
            try {
                timestampToPunch = freshTimestampSource.getAsLong();
            } catch (Throwable th) {
                log.warn("No timestamp was found and attempting to get a fresh timestamp to punch failed."
                        + " Retrying in {} milliseconds.",
                        SafeArg.of("interval", interval),
                        th);
                return;
            }
        }

        try {
            delegate.punch(timestampToPunch);
        } catch (Throwable th) {
            log.warn("Attempt to punch timestamp {} failed. Retrying in {} milliseconds.",
                    SafeArg.of("timestamp", timestampToPunch), SafeArg.of("interval", interval), th);
            lastTimestamp.compareAndSet(INVALID_TIMESTAMP, timestampToPunch);
        }
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
