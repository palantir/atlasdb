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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.sweep.asts.SweepStateCoordinator.SweepableBucket;
import com.palantir.common.concurrent.CoalescingSupplier;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.refreshable.Disposable;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;
import org.immutables.value.Value;

// TODO: Logs and metrics.
public final class DefaultCandidateSweepableBucketRetriever implements CandidateSweepableBucketRetriever {
    private final SafeLogger log = SafeLoggerFactory.get(DefaultCandidateSweepableBucketRetriever.class);
    private final Random random = new Random();
    private final SweepableBucketRetriever sweepableBucketRetriever;
    private final CoalescingSupplier<Optional<TimestampedSweepableBuckets>> supplier;
    private final SettableRefreshable<TimestampedSweepableBuckets> underlyingRefreshable = Refreshable.create(null);
    private final Refreshable<Duration> debouncerDuration;

    private volatile Duration jitter;

    private DefaultCandidateSweepableBucketRetriever(
            SweepableBucketRetriever sweepableBucketRetriever, Refreshable<Duration> debouncerDuration) {
        this.sweepableBucketRetriever = sweepableBucketRetriever;
        this.debouncerDuration = debouncerDuration;
        this.supplier = new CoalescingSupplier<>(this::getNext);
        this.jitter = Duration.ofMillis(random.nextInt(100));
    }

    public static CandidateSweepableBucketRetriever create(
            SweepableBucketRetriever sweepableBucketRetriever, Refreshable<Duration> debouncerDuration) {
        return new DefaultCandidateSweepableBucketRetriever(sweepableBucketRetriever, debouncerDuration);
    }

    @Override
    public void requestUpdate() {
        if (isWithinDebounceWindow()) {
            return; // Quick escape hatch -  we check again in getNext but there's no point spinning up a task on
            // an executor if we know it's going to fail.
        }

        ListenableFuture<Optional<TimestampedSweepableBuckets>> future = supplier.getAsync();
        Futures.addCallback(future, new SetRefreshable(), MoreExecutors.directExecutor());
    }

    @Override
    public Disposable subscribeToChanges(Consumer<List<SweepableBucket>> task) {
        // The refreshable stores a Timestamped list, so that new updates trigger a subscription update, even if
        // the list of sweepable buckets is the same across refreshes.
        return underlyingRefreshable.subscribe(value -> task.accept(value.sweepableBuckets()));
    }

    private Optional<TimestampedSweepableBuckets> getNext() {
        if (isWithinDebounceWindow()) {
            log.info(
                    "Skipping sweepable bucket update as we are within the debounce window. This is interesting because we passed the requestUpdate check.");
            return Optional.empty();
        }
        List<SweepableBucket> sweepableBuckets = sweepableBucketRetriever.getSweepableBuckets();
        log.info("Retrieved sweepable buckets.", SafeArg.of("buckets", sweepableBuckets));
        TimestampedSweepableBuckets timestampedBuckets = ImmutableTimestampedSweepableBuckets.builder()
                .sweepableBuckets(sweepableBuckets)
                .timestamp(Instant.now())
                .build();

        setJitter();

        return Optional.of(timestampedBuckets);
    }

    private boolean isWithinDebounceWindow() {
        TimestampedSweepableBuckets existing = underlyingRefreshable.get();
        return existing != null
                && existing.timestamp()
                        .isAfter(Instant.now().minus(debouncerDuration.get().plus(jitter)));
    }

    private void setJitter() {
        jitter = Duration.ofMillis(random.nextInt(100));
        log.trace("Added jitter to debounce duration", SafeArg.of("jitter", jitter.toMillis()));
    }

    @Value.Immutable
    interface TimestampedSweepableBuckets {
        @Value.Parameter
        List<SweepableBucket> sweepableBuckets();

        @Value.Parameter
        Instant timestamp();
    }

    // TODO(mdaudali): Make this a singleton. Using an enum isn't an option at first glance since we can't
    //  then access log and underlyingRefreshable.
    private final class SetRefreshable implements FutureCallback<Optional<TimestampedSweepableBuckets>> {
        @Override
        public void onSuccess(Optional<TimestampedSweepableBuckets> result) {
            // If not present, we decided not to update on this run.
            result.ifPresent(underlyingRefreshable::update);
        }

        @Override
        public void onFailure(Throwable t) {
            log.warn(
                    "There was an error retrieving the latest sweep buckets. The next request to update the sweepable buckets will retry the operation, but this likely indicates a bug.",
                    t);
        }
    }
}
