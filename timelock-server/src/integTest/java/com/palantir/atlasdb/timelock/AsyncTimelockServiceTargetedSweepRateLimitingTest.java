/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;

import static com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy.CONSERVATIVE;
import static com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy.THOROUGH;
import static com.palantir.atlasdb.timelock.lock.TargetedSweepLockDecorator.LOCK_ACQUIRES_PER_SECOND;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.data.Offset;
import org.junit.Test;

import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.sweep.queue.ShardAndStrategy;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.v2.LockRequest;
import com.palantir.lock.v2.LockResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.util.Pair;

public class AsyncTimelockServiceTargetedSweepRateLimitingTest extends AbstractAsyncTimelockServiceIntegrationTest {

    private static final String CLIENT = UUID.randomUUID().toString();
    private static final String RATE_LIMITED_CLIENT = "should-rate-limit";
    private static final long TIMEOUT = Duration.ofMillis(100).toMillis();

    private ListeningScheduledExecutorService executorService = getExecutorService();
    private ListeningScheduledExecutorService executorService2 = getExecutorService();

    @Test
    public void full_ete() throws InterruptedException {
        Pair<Meter, ListenableFuture<?>> nonTsLock = run(() -> nonTsLockRequest(), CLIENT, executorService);
        Pair<Meter, ListenableFuture<?>> nonTsLock2 = run(() -> nonTsLockRequest(), CLIENT, executorService);
        Pair<Meter, ListenableFuture<?>> tsLock =
                run(ShardAndStrategy.of(0, CONSERVATIVE), CLIENT, executorService);
        Pair<Meter, ListenableFuture<?>> tsLockDuplicated =
                run(ShardAndStrategy.of(0, CONSERVATIVE), CLIENT, executorService);
        Pair<Meter, ListenableFuture<?>> tsLock2 =
                run(ShardAndStrategy.of(0, THOROUGH), CLIENT, executorService);

        Pair<Meter, ListenableFuture<?>> nonTsLock_r =
                run(() -> nonTsLockRequest(), RATE_LIMITED_CLIENT, executorService2);
        Pair<Meter, ListenableFuture<?>> nonTsLock2_r =
                run(() -> nonTsLockRequest(), RATE_LIMITED_CLIENT, executorService2);
        Pair<Meter, ListenableFuture<?>> tsLock_r =
                run(ShardAndStrategy.of(0, CONSERVATIVE), RATE_LIMITED_CLIENT, executorService2);
        Pair<Meter, ListenableFuture<?>> tsLockDuplicated_r =
                run(ShardAndStrategy.of(0, CONSERVATIVE), RATE_LIMITED_CLIENT, executorService2);
        Pair<Meter, ListenableFuture<?>> tsLock2_r =
                run(ShardAndStrategy.of(0, THOROUGH), RATE_LIMITED_CLIENT, executorService2);

        List<Pair<Meter, ListenableFuture<?>>> nonRateLimitedRuns =
                ImmutableList.of(nonTsLock, nonTsLock2, tsLock, tsLockDuplicated, tsLock2);

        List<Pair<Meter, ListenableFuture<?>>> rateLimitedRuns =
                ImmutableList.of(nonTsLock_r, nonTsLock2_r, tsLock_r, tsLockDuplicated_r, tsLock2_r);

        Future<List<Object>> combinedFuture = asFuture(Iterables.concat(nonRateLimitedRuns, rateLimitedRuns));

        Thread.sleep(15_000);
        combinedFuture.cancel(true);

        SoftAssertions softAssertions = new SoftAssertions();
        softAssertions.assertThat(meanRates(nonRateLimitedRuns))
                .as("None of these should be rate limited")
                .allSatisfy(AsyncTimelockServiceTargetedSweepRateLimitingTest::isNotRateLimited);

        List<Double> rateLimitedRunMeanRates = meanRates(rateLimitedRuns);
        softAssertions.assertThat(rateLimitedRunMeanRates.subList(0, 2))
                .as("first two non ts lock requests")
                .allSatisfy(AsyncTimelockServiceTargetedSweepRateLimitingTest::isNotRateLimited);

        softAssertions.assertThat(rateLimitedRunMeanRates.get(4))
                .as("last ts non-shared request is in rate limited correctly")
                .isLessThan(LOCK_ACQUIRES_PER_SECOND + Offset.offset(0.3).value);

        softAssertions.assertThat(rateLimitedRunMeanRates.get(2) + rateLimitedRunMeanRates.get(3))
                .as("adding rates for two separate requests for same lock should be within rate limit")
                .isLessThan(LOCK_ACQUIRES_PER_SECOND + Offset.offset(0.3).value);

        softAssertions.assertAll();
    }

    private static ListeningScheduledExecutorService getExecutorService() {
        ScheduledThreadPoolExecutor delegate = PTExecutors.newScheduledThreadPoolExecutor(100);
        return MoreExecutors.listeningDecorator(delegate);
    }

    private static List<Double> meanRates(List<Pair<Meter, ListenableFuture<?>>> pairs) {
        return pairs.stream().map(Pair::getLhSide).map(Meter::getMeanRate).collect(Collectors.toList());
    }

    private static void isNotRateLimited(Double rate) {
        assertThat(rate).isGreaterThan(LOCK_ACQUIRES_PER_SECOND);
    }

    private static ListenableFuture<List<Object>> asFuture(Iterable<Pair<Meter, ListenableFuture<?>>> pairs) {
        return Futures.allAsList(Streams.stream(pairs).map(Pair::getRhSide).collect(Collectors.toList()));
    }

    private Pair<Meter, ListenableFuture<?>> run(
            ShardAndStrategy shardAndStrategy,
            String client,
            ListeningScheduledExecutorService executorService) {
        return run(shardAndStrategy::toLockDescriptor, client, executorService);
    }

    private Pair<Meter, ListenableFuture<?>> run(
            Supplier<LockDescriptor> descriptor,
            String client,
            ListeningScheduledExecutorService executorService) {
        Meter meter = new Meter();
        AtomicReference<LockToken> lastHeldToken = new AtomicReference<>();
        ListenableFuture<?> future = executorService.scheduleWithFixedDelay(() -> {
            LockResponse response = cluster.timelockServiceForClient(client).lock(requestFor(descriptor.get()));
            if (!response.wasSuccessful()) {
                return;
            }
            try {
                lastHeldToken.set(response.getToken());
                meter.mark();
            } finally {
                if (lastHeldToken.get() != null) {
                    // synchronous unlocking  is bad for us here!
                    cluster.unlockerForClient(client).enqueue(ImmutableSet.of(lastHeldToken.get()));
                }
            }
        }, 0, 50, TimeUnit.MILLISECONDS);

        return new Pair<>(meter, future);
    }

    private static LockRequest requestFor(LockDescriptor... locks) {
        return LockRequest.of(ImmutableSet.copyOf(locks), TIMEOUT);
    }

    private static LockDescriptor nonTsLockRequest() {
        return StringLockDescriptor.of(UUID.randomUUID().toString());
    }

}
