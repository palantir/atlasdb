/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.performance.benchmarks;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionService.LeadershipToken;
import com.palantir.leader.proxy.AwaitingLeadershipProxy;
import com.palantir.leader.proxy.LeadershipCoordinator;
import com.palantir.proxy.annotations.Proxy;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

@Measurement(iterations = 10, time = 2)
@Warmup(iterations = 6, time = 1)
@Fork(value = 1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class AwaitingLeadershipProxyBenchmark {
    private final LeaderAwareService service = AwaitingLeadershipProxy.newProxyInstance(
            LeaderAwareServiceProxy::create,
            LeaderAwareService.class,
            () -> LeaderAwareImpl.INSTANCE,
            LeadershipCoordinator.create(FakeLeaderElectionService.INSTANCE));
    private static final int ASYNC_ITERATIONS = 1000;

    @Benchmark
    @Threads(256)
    public int benchmarkBlocking() {
        return service.somethingBlocking();
    }

    @Benchmark
    @OperationsPerInvocation(ASYNC_ITERATIONS)
    @Threads(12)
    public List<Object> benchmarkAsync() {
        List<ListenableFuture<?>> futures = new ArrayList<>(ASYNC_ITERATIONS);
        for (int i = 0; i < ASYNC_ITERATIONS; i++) {
            futures.add(service.somethingAsync());
        }
        ListenableFuture<List<Object>> composition = Futures.allAsList(futures);
        return Futures.getUnchecked(composition);
    }

    @Proxy
    public interface LeaderAwareService extends Closeable {
        int somethingBlocking();

        ListenableFuture<?> somethingAsync();
    }

    private enum LeaderAwareImpl implements LeaderAwareService {
        INSTANCE;

        @Override
        public int somethingBlocking() {
            return ThreadLocalRandom.current().nextInt();
        }

        @Override
        public ListenableFuture<?> somethingAsync() {
            return Futures.immediateFuture(somethingBlocking());
        }

        @Override
        public void close() {}
    }

    private enum SingletonLeadershipToken implements LeadershipToken {
        INSTANCE;

        @Override
        public boolean sameAs(LeadershipToken token) {
            return token == INSTANCE;
        }
    }

    private enum FakeLeaderElectionService implements LeaderElectionService {
        INSTANCE;

        private static final ListeningScheduledExecutorService executor =
                MoreExecutors.listeningDecorator(PTExecutors.newScheduledThreadPool(4));

        @Override
        public void markNotEligibleForLeadership() {}

        @Override
        public boolean hostileTakeover() {
            return false;
        }

        @Override
        public LeadershipToken blockOnBecomingLeader() {
            return SingletonLeadershipToken.INSTANCE;
        }

        @Override
        public Optional<LeadershipToken> getCurrentTokenIfLeading() {
            return Optional.of(SingletonLeadershipToken.INSTANCE);
        }

        @Override
        public ListenableFuture<StillLeadingStatus> isStillLeading(LeadershipToken providedToken) {
            return executor.schedule(() -> StillLeadingStatus.LEADING, 2, TimeUnit.MILLISECONDS);
        }

        @Override
        public boolean stepDown() {
            return false;
        }

        @Override
        public Optional<HostAndPort> getRecentlyPingedLeaderHost() {
            return Optional.empty();
        }
    }
}
