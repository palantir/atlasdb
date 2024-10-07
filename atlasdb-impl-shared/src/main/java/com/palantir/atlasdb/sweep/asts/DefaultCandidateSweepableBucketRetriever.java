/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.asts;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Disposable;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.immutables.value.Value;

// The overall design of this class is to effectively debounce requests to update the sweepable buckets.
// By abstracting away the logic of how and when the buckets will be updated, downstream classes can call requestUpdate
// without needing to do any bookkeeping of their own.
public final class DefaultCandidateSweepableBucketRetriever implements CandidateSweepableBucketRetriever {
    private final SafeLogger log = SafeLoggerFactory.get(DefaultCandidateSweepableBucketRetriever.class);
    private final SweepableBucketRetriever sweepableBucketRetriever;
    private final CoalescingSupplier<Void> supplier;
    private final SettableRefreshable<TimestampedSweepableBuckets> underlyingRefreshable = Refreshable.create(null);

    // This should be non-zero - see the comment above the coalescing supplier in the constructor.
    // But we've not preconditioning on it since zero is acceptable for testing.
    // Updates to this will not be reflected until a subsequent refresh, as we store the duration at the time
    // of the refresh in the TimestampedSweepableBuckets
    private final Refreshable<Duration> minimumDurationBetweenRefresh;
    // We use a supplier for the jitter (rather than directly using ThreadLocalRandom) to make testing simpler.
    // We use a jitter to prevent thundering herd issues at the macro scale. The underlying SweepableBucketRetriever
    // should have jitter between requests, but we also want to have the batch requests be scheduled at different times
    // across nodes.
    private final Supplier<Long> jitterMillisGenerator;

    // Passed in for testing
    private final Clock clock;

    @VisibleForTesting
    DefaultCandidateSweepableBucketRetriever(
            SweepableBucketRetriever sweepableBucketRetriever,
            Refreshable<Duration> minimumDurationBetweenRefresh,
            Clock clock,
            Supplier<Long> jitterMillisGenerator) {
        this.sweepableBucketRetriever = sweepableBucketRetriever;
        this.minimumDurationBetweenRefresh = minimumDurationBetweenRefresh;
        this.clock = clock;

        // Coalescing Supplier doesn't have the exact semantics we want - for requests that come in after the
        // computation has started, they will trigger a new computation after the first one is complete. However,
        // we would like to have the request to be a no-op if there's already one in progress (rather than trigger
        // a new request).
        // Despite that, it's simpler to use the battle tested coalescing supplier, and mitigate this with the already
        // needed minimumDurationBetweenRefresh, since the next iteration will happen straight after
        // which should still be within the debounce window.
        this.supplier = new CoalescingSupplier<>(this::getNext);
        this.jitterMillisGenerator = jitterMillisGenerator;
    }

    public static CandidateSweepableBucketRetriever create(
            SweepableBucketRetriever sweepableBucketRetriever,
            Refreshable<Duration> debouncerDuration,
            Refreshable<Duration> maxJitter,
            Clock clock) {
        return new DefaultCandidateSweepableBucketRetriever(sweepableBucketRetriever, debouncerDuration, clock, () -> {
            // This is not full jitter. I chose to require a minimum delay, because we simply _do not need_ to refresh
            // buckets that quickly, and the scans over the database aren't precisely free.
            // This jitter only really matters for things calling requestUpdate frequently (as opposed to the background
            // task, which should have its own jitter)
            return ThreadLocalRandom.current().nextLong(0, maxJitter.get().toMillis());
        });
    }

    @Override
    public void requestUpdate() {
        if (isWithinDebounceWindow()) {
            return; // Quick escape hatch -  we check again in getNext but there's no point spinning up a task on
            // an executor if we know it's going to fail.
        }

        supplier.getAsync();
    }

    @Override
    public Disposable subscribeToChanges(Consumer<Set<SweepableBucket>> task) {
        // The refreshable stores a Timestamped list, so that new updates trigger a subscription update, even if
        // the list of sweepable buckets is the same across refreshes - refreshable would otherwise skip the subscriber
        // update if the underlying value is equal.
        return underlyingRefreshable.subscribe(value -> {
            if (value != null) {
                task.accept(value.sweepableBuckets());
            }
        });
    }

    // We have to return _something_ to get Coalescing supplier to be typed.
    // We process and store the value within the context of the coalescing supplier, as we would otherwise set the
    // refreshable value too late, and so it's possible to have another round before you update the refreshable
    // This is especially important given that it's highly likely the coalescing supplier will immediately execute
    // straight after the first one completes, given the semantics described at the top of this class.
    private Void getNext() {
        if (isWithinDebounceWindow()) {
            return null;
        }
        try {
            Set<SweepableBucket> sweepableBuckets = sweepableBucketRetriever.getSweepableBuckets();
            // TODO(mdaudali): We may want to cap the number of buckets being logged, or simply log the number of
            //  buckets once we actually ship this, to avoid destroying our logging infrastructure.
            //  For now, the telemetry will be useful.
            log.info("Retrieved sweepable buckets.", SafeArg.of("buckets", sweepableBuckets));
            underlyingRefreshable.update(ImmutableTimestampedSweepableBuckets.builder()
                    .sweepableBuckets(sweepableBuckets)
                    .noRefreshBeforeInclusive(Instant.now(clock)
                            .plus(minimumDurationBetweenRefresh.get())
                            .plusMillis(jitterMillisGenerator.get()))
                    .build());

        } catch (Exception e) {
            // It possibly indicates a bug because in the current implementation, it's expected that the underlying
            // sweepable bucket retriever will fail silently on a given shard, rather than bubbling up the exception.
            log.warn(
                    "There was an error retrieving the latest sweep buckets. The next request to update the"
                            + " sweepable buckets will retry the operation, but this likely indicates a bug.",
                    e);
        }
        return null;
    }

    // Note: This window is inclusive (so you cannot issue a refresh at timestamp + duration + jitter exactly)
    // There was no particular reason why this was chosen, but if you change it, you'll need to change min delay
    // in tests from zero to one millis (see the reason above coalescing supplier)
    private boolean isWithinDebounceWindow() {
        TimestampedSweepableBuckets existing = underlyingRefreshable.get();
        return existing != null && !Instant.now(clock).isAfter(existing.noRefreshBeforeInclusive());
    }

    @Value.Immutable
    interface TimestampedSweepableBuckets {
        @Value.Parameter
        Set<SweepableBucket> sweepableBuckets();

        @Value.Parameter
        Instant noRefreshBeforeInclusive();
    }
}
