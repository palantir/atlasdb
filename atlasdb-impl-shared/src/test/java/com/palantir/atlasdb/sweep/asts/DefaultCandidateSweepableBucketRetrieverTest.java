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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.lenient;

import com.palantir.atlasdb.sweep.asts.SweepableBucket.TimestampRange;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.atlasdb.table.description.SweeperStrategy;
import com.palantir.refreshable.Disposable;
import com.palantir.refreshable.Refreshable;
import com.palantir.refreshable.SettableRefreshable;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DefaultCandidateSweepableBucketRetrieverTest {
    private static final Set<SweepableBucket> BUCKETS = Set.of(
            SweepableBucket.of(
                    Bucket.of(ShardAndStrategy.of(1, SweeperStrategy.CONSERVATIVE), 1), TimestampRange.of(1, 3)),
            SweepableBucket.of(
                    Bucket.of(ShardAndStrategy.of(2, SweeperStrategy.THOROUGH), 2), TimestampRange.of(4, 6)));
    private static final SweepableBucketRetriever WITH_BUCKETS = () -> BUCKETS;

    private final SettableRefreshable<Duration> minimumDurationBetweenRefresh = Refreshable.create(Duration.ZERO);
    private final SettableRefreshable<Long> jitter = Refreshable.create(0L);

    private volatile Duration currentTime = Duration.ZERO;

    @Mock
    private Clock clock;

    @BeforeEach
    public void before() {
        lenient().when(clock.instant()).thenAnswer(invocation -> Instant.ofEpochMilli(currentTime.toMillis()));
    }

    @Test
    public void addingSubscriberBeforeRetrieverRunsDoesNotCallSubscriber() {
        AtomicInteger counter = new AtomicInteger();
        CandidateSweepableBucketRetriever candidateBucketRetriever = candidateRetriever(WITH_BUCKETS);
        candidateBucketRetriever.subscribeToChanges(_ignored -> counter.incrementAndGet());

        // It's possible for this to spuriously pass if the subscriber is called after the test checks the counter,
        // as there's no other way to know when the task is complete. This issue does plague other tests that assert
        // "nothing happening". The tryWaitUntilAsserted method adds a minor delay before checking, which should help
        // catch a bad implementation early.
        tryWaitUntilAsserted(() -> assertThat(counter.get()).isEqualTo(0));
    }

    @Test
    public void firstCallRequestsNewBucketsAndUpdatesAllSubscribersInOrder() {
        List<Integer> callbackTracker = new ArrayList<>();
        CandidateSweepableBucketRetriever candidateBucketRetriever = candidateRetriever(WITH_BUCKETS);

        candidateBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(() -> callbackTracker.add(1), BUCKETS));
        candidateBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(() -> callbackTracker.add(2), BUCKETS));

        candidateBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(callbackTracker).containsExactly(1, 2));
    }

    @Test
    public void refreshingToTheSameBucketListStillUpdatesSubscribers() {
        AtomicInteger counter = new AtomicInteger();
        CandidateSweepableBucketRetriever candidateBucketRetriever = candidateRetriever(WITH_BUCKETS);

        candidateBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(counter::incrementAndGet, BUCKETS));

        candidateBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(1));

        tickTime(Duration.ofMillis(1));
        candidateBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(2));
    }

    @Test
    public void failedRequestDoesNotUpdateSubscriber() {
        AtomicInteger counter = new AtomicInteger();
        CandidateSweepableBucketRetriever candidateSweepableBucketRetriever = candidateRetriever(() -> {
            throw new RuntimeException("FAILED");
        });

        candidateSweepableBucketRetriever.subscribeToChanges(_ignored -> counter.incrementAndGet());
        candidateSweepableBucketRetriever.requestUpdate();

        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(0));
    }

    @Test
    public void failedRequestDoesNotBlockSubsequentRequestFromStartingWithinDelay() {
        AtomicInteger counter = new AtomicInteger();
        AtomicBoolean shouldFail = new AtomicBoolean(true);

        CandidateSweepableBucketRetriever candidateSweepableBucketRetriever = candidateRetriever(() -> {
            if (shouldFail.get()) {
                throw new RuntimeException("FAILED");
            }
            return BUCKETS;
        });

        minimumDurationBetweenRefresh.update(Duration.ofDays(1));
        candidateSweepableBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(counter::incrementAndGet, BUCKETS));
        candidateSweepableBucketRetriever.requestUpdate();

        shouldFail.set(false);
        // Despite the 1-day wait between refreshes - we should be able to refresh given the previous failure.
        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(1));
    }

    @Test // Note, interestingly this also showed the bug on the callback coming after the coalescing supplier
    public void subsequentRequestsWhileRetrievingBucketsDoNothing() throws InterruptedException {
        AtomicInteger numCallbacks = new AtomicInteger();
        AtomicInteger numRequests = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);
        CountDownLatch hasStarted = new CountDownLatch(1);

        CandidateSweepableBucketRetriever candidateSweepableBucketRetriever = candidateRetriever(() -> {
            hasStarted.countDown();
            awaitLatch(latch);
            numRequests.incrementAndGet();
            return BUCKETS;
        });
        candidateSweepableBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(numCallbacks::incrementAndGet, BUCKETS));

        candidateSweepableBucketRetriever.requestUpdate();
        assertThat(hasStarted.await(1, TimeUnit.SECONDS)).isTrue();

        candidateSweepableBucketRetriever.requestUpdate();

        latch.countDown();
        tryWaitUntilAsserted(() -> assertThat(numCallbacks).hasValue(1));
        assertThat(numRequests).hasValue(1);
    }

    @Test
    public void firstRequestAfterDebounceDurationPassesRetrievesBuckets() {
        List<Integer> callbackTracker = new ArrayList<>();
        AtomicReference<Set<SweepableBucket>> buckets = new AtomicReference<>(Set.of());
        CandidateSweepableBucketRetriever candidateSweepableBucketRetriever = candidateRetriever(buckets::get);

        minimumDurationBetweenRefresh.update(Duration.ofDays(1));
        candidateSweepableBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(() -> callbackTracker.add(1), Set.of()));
        candidateSweepableBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(() -> callbackTracker.add(2), BUCKETS));

        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(callbackTracker).containsExactly(1));

        tickTime(minimumDurationBetweenRefresh.get().plusMillis(1));
        buckets.set(BUCKETS);

        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(callbackTracker).containsExactly(1, 2));
    }

    @Test
    public void subsequentRequestsAfterRetrievingBucketsBeforeDebounceDurationDoesNothing() {
        AtomicInteger numCallbacks = new AtomicInteger();
        AtomicInteger numRequests = new AtomicInteger();
        CandidateSweepableBucketRetriever candidateSweepableBucketRetriever = candidateRetriever(() -> {
            numRequests.incrementAndGet();
            return BUCKETS;
        });
        candidateSweepableBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(numCallbacks::incrementAndGet, BUCKETS));

        minimumDurationBetweenRefresh.update(Duration.ofDays(1));
        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(numCallbacks).hasValue(1));

        candidateSweepableBucketRetriever.requestUpdate();
        candidateSweepableBucketRetriever.requestUpdate();

        assertThat(numCallbacks).hasValue(1);
        assertThat(numRequests).hasValue(1);
    }

    @Test
    public void debounceDurationAddsJitter() {
        AtomicInteger counter = new AtomicInteger();
        minimumDurationBetweenRefresh.update(Duration.ofMillis(854));
        jitter.update(786L);
        CandidateSweepableBucketRetriever candidateSweepableBucketRetriever = candidateRetriever(WITH_BUCKETS);
        candidateSweepableBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(counter::incrementAndGet, BUCKETS));

        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(1));
        tickTime(minimumDurationBetweenRefresh
                .get()
                .plusMillis(jitter.get())); // The wait is inclusive, so we still shouldn't trigger another run
        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(1));

        tickTime(Duration.ofMillis(1));
        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(2));
    }

    @Test
    public void jitterDoesNotChangeUntilSuccessfulRefresh() {
        AtomicInteger counter = new AtomicInteger();
        minimumDurationBetweenRefresh.update(Duration.ofMillis(100));
        jitter.update(11L);

        CandidateSweepableBucketRetriever candidateSweepableBucketRetriever = candidateRetriever(WITH_BUCKETS);
        candidateSweepableBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(counter::incrementAndGet, BUCKETS));

        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(1));

        jitter.update(0L);
        tickTime(
                minimumDurationBetweenRefresh
                        .get()); // If we were using the new jitter, this would cause us to have a successful update,
        // but we're not.
        candidateSweepableBucketRetriever.requestUpdate();

        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(1));

        tickTime(Duration.ofMillis(12L));
        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(2));
    }

    @Test
    public void debounceDurationJitterChangesAfterEachRefresh() {
        AtomicInteger counter = new AtomicInteger();
        minimumDurationBetweenRefresh.update(Duration.ofMillis(100));
        jitter.update(11L);
        CandidateSweepableBucketRetriever candidateSweepableBucketRetriever = candidateRetriever(WITH_BUCKETS);
        candidateSweepableBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(counter::incrementAndGet, BUCKETS));

        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(1));

        tickTime(minimumDurationBetweenRefresh.get().plusMillis(jitter.get() + 1));
        jitter.update(25L); // not used until after the refresh succeeds
        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(2));

        tickTime(minimumDurationBetweenRefresh.get().plusMillis(jitter.get() + 1));
        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(3));
    }

    @Test
    public void newMinimumDurationAppliesAfterNextRefresh() {
        AtomicInteger numCallbacks = new AtomicInteger();
        CandidateSweepableBucketRetriever candidateSweepableBucketRetriever = candidateRetriever(WITH_BUCKETS);

        minimumDurationBetweenRefresh.update(Duration.ofDays(1));
        candidateSweepableBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(numCallbacks::incrementAndGet, BUCKETS));

        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(numCallbacks).hasValue(1));

        // This should no-op
        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(numCallbacks).hasValue(1));

        tickTime(Duration.ofMillis(1)); // Inclusive wait.
        minimumDurationBetweenRefresh.update(Duration.ZERO);
        candidateSweepableBucketRetriever.requestUpdate();
        // New update hasn't applied
        tryWaitUntilAsserted(() -> assertThat(numCallbacks).hasValue(1));

        tickTime(Duration.ofDays(1)); // The old minimum duration
        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(numCallbacks).hasValue(2));

        tickTime(Duration.ofMillis(1)); // Now the new min duration is applied
        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(numCallbacks).hasValue(3));
    }

    @Test
    public void disposedCallbackNoLongerRuns() {
        AtomicInteger counter = new AtomicInteger();
        CandidateSweepableBucketRetriever candidateSweepableBucketRetriever = candidateRetriever(WITH_BUCKETS);

        Disposable disposable = candidateSweepableBucketRetriever.subscribeToChanges(
                runCallbackIfBucketsMatchExpected(counter::incrementAndGet, BUCKETS));
        candidateSweepableBucketRetriever.subscribeToChanges(runCallbackIfBucketsMatchExpected(
                () -> {
                    counter.addAndGet(2);
                },
                BUCKETS));

        candidateSweepableBucketRetriever.requestUpdate();
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(3));

        tickTime(Duration.ofMillis(1));
        disposable.dispose();
        candidateSweepableBucketRetriever.requestUpdate();
        // Incremented by 2, not 1 + 2.
        tryWaitUntilAsserted(() -> assertThat(counter).hasValue(5));
    }

    private Consumer<Set<SweepableBucket>> runCallbackIfBucketsMatchExpected(
            Runnable task, Set<SweepableBucket> expected) {
        return buckets -> {
            if (buckets.equals(expected)) {
                task.run();
            }
        };
    }

    private void awaitLatch(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    // Necessary because the coalescing supplier runs things in an executor, so you have to deal with
    // context switching delays
    private void tryWaitUntilAsserted(ThrowingRunnable task) {
        Awaitility.await()
                .atMost(Duration.ofMillis(100))
                .pollInterval(Duration.ofMillis(10))
                .untilAsserted(task);
    }

    private CandidateSweepableBucketRetriever candidateRetriever(SweepableBucketRetriever retriever) {
        return new DefaultCandidateSweepableBucketRetriever(retriever, minimumDurationBetweenRefresh, clock, jitter);
    }

    private synchronized void tickTime(Duration duration) {
        currentTime = currentTime.plus(duration);
    }
}
