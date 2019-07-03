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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.assertj.core.data.Offset;
import org.junit.After;
import org.junit.Test;

import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
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
    private static final String RATE_LIMITED_CLIENT_CIRCULAR = "should-rate-limit-circular";
    private static final long TIMEOUT = Duration.ofMillis(100).toMillis();

    private final ListeningScheduledExecutorService nonRateLimitedExecutor = getExecutorService();
    private final ListeningScheduledExecutorService rateLimitedExecutor = getExecutorService();

    @After
    public void after() {
        nonRateLimitedExecutor.shutdown();
        rateLimitedExecutor.shutdown();
    }

    @Test
    public void full_ete() throws InterruptedException {
        Pair<Meter, ListenableFuture<?>> nonTsLock = run(() -> nonTsLockRequest(), CLIENT, nonRateLimitedExecutor);
        Pair<Meter, ListenableFuture<?>> nonTsLock2 = run(() -> nonTsLockRequest(), CLIENT, nonRateLimitedExecutor);
        Pair<Meter, ListenableFuture<?>> tsLock =
                run(ShardAndStrategy.of(0, CONSERVATIVE), CLIENT, nonRateLimitedExecutor);
        Pair<Meter, ListenableFuture<?>> tsLockDuplicated =
                run(ShardAndStrategy.of(0, CONSERVATIVE), CLIENT, nonRateLimitedExecutor);
        Pair<Meter, ListenableFuture<?>> tsLock2 =
                run(ShardAndStrategy.of(0, THOROUGH), CLIENT, nonRateLimitedExecutor);

        Pair<Meter, ListenableFuture<?>> nonTsLockRateLimitedClient =
                run(() -> nonTsLockRequest(), RATE_LIMITED_CLIENT, rateLimitedExecutor);
        Pair<Meter, ListenableFuture<?>> nonTsLock2RateLimitedClient =
                run(() -> nonTsLockRequest(), RATE_LIMITED_CLIENT, rateLimitedExecutor);
        Pair<Meter, ListenableFuture<?>> tsLockRateLimitedClient =
                run(ShardAndStrategy.of(0, CONSERVATIVE), RATE_LIMITED_CLIENT, rateLimitedExecutor);
        Pair<Meter, ListenableFuture<?>> tsLockDuplicatedRateLimitedClient =
                run(ShardAndStrategy.of(0, CONSERVATIVE), RATE_LIMITED_CLIENT, rateLimitedExecutor);
        Pair<Meter, ListenableFuture<?>> tsLock2RateLimitedClient =
                run(ShardAndStrategy.of(0, THOROUGH), RATE_LIMITED_CLIENT, rateLimitedExecutor);

        List<Pair<Meter, ListenableFuture<?>>> nonRateLimitedRuns =
                ImmutableList.of(nonTsLock, nonTsLock2, tsLock, tsLockDuplicated, tsLock2);

        List<Pair<Meter, ListenableFuture<?>>> rateLimitedRuns = ImmutableList.of(
                nonTsLockRateLimitedClient,
                nonTsLock2RateLimitedClient,
                tsLockRateLimitedClient,
                tsLockDuplicatedRateLimitedClient,
                tsLock2RateLimitedClient);

        Future<List<Object>> combinedFuture = asFuture(Iterables.concat(nonRateLimitedRuns, rateLimitedRuns));

        Thread.sleep(15_000);
        combinedFuture.cancel(true);

        assertThat(meanRates(nonRateLimitedRuns))
                .as("None of these should be rate limited")
                .allSatisfy(AsyncTimelockServiceTargetedSweepRateLimitingTest::isNotRateLimited);

        assertThat(meanRates(Lists.newArrayList(nonTsLockRateLimitedClient, nonTsLock2RateLimitedClient)))
                .as("first two non ts lock requests are not rate limited")
                .allSatisfy(AsyncTimelockServiceTargetedSweepRateLimitingTest::isNotRateLimited);

        assertThat(meanRate(tsLock2RateLimitedClient))
                .as("last ts non-shared request is in rate limited correctly")
                .satisfies(AsyncTimelockServiceTargetedSweepRateLimitingTest::isRateLimited);

        assertThat(meanRate(tsLockRateLimitedClient) + meanRate(tsLockDuplicatedRateLimitedClient))
                .as("adding rates for two separate requests for same lock should be within rate limit")
                .satisfies(AsyncTimelockServiceTargetedSweepRateLimitingTest::isRateLimited);

    }

    @Test
    public void circular() throws InterruptedException {
        Pair<Meter, ListenableFuture<?>> nonRateLimited = run(
                lockDescriptorSupplier(4, CONSERVATIVE),
                CLIENT,
                nonRateLimitedExecutor);
        Pair<Meter, ListenableFuture<?>> rateLimitedConservative = run(
                lockDescriptorSupplier(4, CONSERVATIVE),
                RATE_LIMITED_CLIENT_CIRCULAR,
                rateLimitedExecutor);

        Pair<Meter, ListenableFuture<?>> rateLimitedConservative2 = run(
                lockDescriptorSupplier(4, CONSERVATIVE),
                RATE_LIMITED_CLIENT_CIRCULAR,
                rateLimitedExecutor);

        Pair<Meter, ListenableFuture<?>> rateLimitedThorough = run(
                lockDescriptorSupplier(8, THOROUGH),
                RATE_LIMITED_CLIENT_CIRCULAR,
                rateLimitedExecutor);

        Pair<Meter, ListenableFuture<?>> rateLimitedThorough2 = run(
                lockDescriptorSupplier(8, THOROUGH),
                RATE_LIMITED_CLIENT_CIRCULAR,
                rateLimitedExecutor);

        Future<List<Object>> combinedFuture = asFuture(ImmutableList.of(
                nonRateLimited,
                rateLimitedConservative,
                rateLimitedConservative2,
                rateLimitedThorough,
                rateLimitedThorough2));
        Thread.sleep(15_000);
        combinedFuture.cancel(true);

        assertThat(meanRate(nonRateLimited))
                .as("non rate limited client isn't rate limited when we go in a circle like targeted sweep")
                .satisfies(AsyncTimelockServiceTargetedSweepRateLimitingTest::isNotRateLimited);

        assertThat(ImmutableList.of(meanRate(rateLimitedThorough), meanRate(rateLimitedThorough2)))
                .as("thorough is rate limited with 8 shards and 2 threads, each 'thread' should get 2 per second per thread")
                .allSatisfy(AsyncTimelockServiceTargetedSweepRateLimitingTest::isRateLimited);

        assertThat(meanRate(rateLimitedConservative) + meanRate(rateLimitedConservative2))
                .as("conservative is rate limited with 4 shards and 1 thread, each 'thread' should get 1 per second per thread")
                .satisfies(AsyncTimelockServiceTargetedSweepRateLimitingTest::isRateLimited);
    }

    private static Supplier<LockDescriptor> lockDescriptorSupplier(
            int numShards,
            SweepStrategy sweepStrategy) {
        AtomicInteger counter = new AtomicInteger(0);
        return () -> {
            int shard = counter.getAndIncrement() % numShards;
            ShardAndStrategy shardStrategy = ShardAndStrategy.of(shard, sweepStrategy);
            return shardStrategy.toLockDescriptor();
        };
    }

    private static ListeningScheduledExecutorService getExecutorService() {
        ScheduledThreadPoolExecutor delegate = PTExecutors.newScheduledThreadPoolExecutor(10);
        return MoreExecutors.listeningDecorator(delegate);
    }

    private static List<Double> meanRates(Iterable<Pair<Meter, ListenableFuture<?>>> pairs) {
        return Streams.stream(pairs).map(Pair::getLhSide).map(Meter::getMeanRate).collect(Collectors.toList());
    }

    private static Double meanRate(Pair<Meter, ListenableFuture<?>> pair) {
        return pair.lhSide.getMeanRate();
    }

    private static void isNotRateLimited(Double rate) {
        assertThat(rate).isGreaterThan(LOCK_ACQUIRES_PER_SECOND);
    }

    private static void isRateLimited(double rate) {
        assertThat(rate).isCloseTo(LOCK_ACQUIRES_PER_SECOND, Offset.offset(0.3));
    }

    private static ListenableFuture<List<Object>> asFuture(Iterable<Pair<Meter, ListenableFuture<?>>> pairs) {
        return Futures.allAsList(Streams.stream(pairs).map(Pair::getRhSide).collect(Collectors.toList()));
    }

    private Pair<Meter, ListenableFuture<?>> run(
            ShardAndStrategy shardAndStrategy,
            String client,
            ListeningScheduledExecutorService executor) {
        return run(shardAndStrategy::toLockDescriptor, client, executor);
    }

    private Pair<Meter, ListenableFuture<?>> run(
            Supplier<LockDescriptor> descriptor,
            String client,
            ListeningScheduledExecutorService executor) {
        Meter meter = new Meter();
        AtomicReference<LockToken> lastHeldToken = new AtomicReference<>();
        ListenableFuture<?> future = executor.scheduleWithFixedDelay(() -> {
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
