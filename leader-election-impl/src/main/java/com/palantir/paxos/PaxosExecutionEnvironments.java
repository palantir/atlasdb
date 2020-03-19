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

package com.palantir.paxos;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.common.concurrent.MultiplexingCompletionService;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.Preconditions;
import com.palantir.paxos.PaxosExecutionEnvironment.ExecutionContext;

public final class PaxosExecutionEnvironments {

    private PaxosExecutionEnvironments() { }

    public static <S> PaxosExecutionEnvironment<S> threadPerService(
            List<S> services,
            Map<? extends S, ExecutorService> executors) {
        return new AllRequestsOnSeparateThreads<>(services, executors);
    }

    public static <S> PaxosExecutionEnvironment<S> useCurrentThreadForLocalService(
            LocalAndRemotes<S> localAndRemotes,
            Map<? extends S, ExecutorService> executors) {
        return new UseCurrentThreadForLocalExecution<>(localAndRemotes, executors);
    }

    public static <S> PaxosExecutionEnvironment<S> useCurrentThreadForLocalService(
            LocalAndRemotes<S> localAndRemotes,
            ExecutorService executor) {
        return useCurrentThreadForLocalService(localAndRemotes, localAndRemotes.withSharedExecutor(executor));
    }

    public static <S> PaxosExecutionEnvironment<S> asyncExecutionEnvironment(List<S> services) {
        return new AsyncExecutionEnvironment<>(services);
    }

    private static final class AllRequestsOnSeparateThreads<T> implements PaxosExecutionEnvironment<T> {

        private final List<T> services;
        private final Map<? extends T, ExecutorService> executors;

        private AllRequestsOnSeparateThreads(List<T> services, Map<? extends T, ExecutorService> executors) {
            Preconditions.checkState(executors.keySet().containsAll(Sets.newHashSet(services)),
                    "Each service should have an executor.");
            this.services = services;
            this.executors = executors;
        }

        @Override
        public int numberOfServices() {
            return services.size();
        }

        @Override
        public <R extends PaxosResponse> ExecutionContext<T, R> execute(Function<T, R> request) {
            MultiplexingCompletionService<T, R> responseCompletionService =
                    MultiplexingCompletionService.create(executors);
            // kick off all the requests
            List<ListenableFuture<Map.Entry<T, R>>> allFutures = Lists.newArrayList();
            Queue<PaxosExecutionEnvironment.Result<T, R>> submissionFailures = Lists.newLinkedList();
            for (T remote : services) {
                try {
                    allFutures.add(responseCompletionService.submit(remote, () -> request.apply(remote)));
                } catch (RejectedExecutionException e) {
                    submissionFailures.add(Results.failure(e));
                }
            }
            return new ExecutionContextImpl<>(
                    submissionFailures,
                    PollOnlyCompletionService.fromMultiplexing(responseCompletionService),
                    allFutures);
        }

        @Override
        public <R extends PaxosResponse> ExecutionContext<T, R> executeAsync(AsyncFunction<T, R> function) {
            return execute(service -> AtlasFutures.getUnchecked(AtlasFutures.call(function, service)));
        }

        @Override
        public <T1> PaxosExecutionEnvironment<T1> map(Function<T, T1> mapper) {
            BiMap<T, T1> mapping = KeyedStream.of(services).map(mapper).collectTo(HashBiMap::create);

            Map<T1, ExecutorService> newExecutors = KeyedStream.stream(executors)
                    .mapKeys(mapping::get)
                    .collectToMap();

            List<T1> newServices = services.stream().map(mapping::get).collect(Collectors.toList());
            return new AllRequestsOnSeparateThreads<>(newServices, newExecutors);
        }
    }

    private static final class UseCurrentThreadForLocalExecution<T> implements PaxosExecutionEnvironment<T> {

        private static final Logger log = LoggerFactory.getLogger(UseCurrentThreadForLocalExecution.class);

        private final LocalAndRemotes<T> localAndRemotes;
        private final Map<? extends T, ExecutorService> executors;
        private final AllRequestsOnSeparateThreads<T> remoteExecutionEnvironment;

        private UseCurrentThreadForLocalExecution(
                LocalAndRemotes<T> localAndRemotes,
                Map<? extends T, ExecutorService> executors) {
            this.localAndRemotes = localAndRemotes;
            this.executors = executors;
            this.remoteExecutionEnvironment =
                    new AllRequestsOnSeparateThreads<>(localAndRemotes.remotes(), executors);
        }

        @Override
        public int numberOfServices() {
            return localAndRemotes.all().size();
        }

        @Override
        public <R extends PaxosResponse> ExecutionContext<T, R> execute(Function<T, R> function) {
            ExecutionContext<T, R> remoteExecutionContext = this.remoteExecutionEnvironment.execute(function);
            return remoteExecutionContext.withExistingResults(ImmutableList.of(executeLocally(function)));
        }

        @Override
        public <R extends PaxosResponse> ExecutionContext<T, R> executeAsync(
                AsyncFunction<T, R> function) {
            return execute(service -> AtlasFutures.getUnchecked(AtlasFutures.call(function, service)));
        }

        @Override
        public <T1> PaxosExecutionEnvironment<T1> map(Function<T, T1> mapper) {
            BiMap<T, T1> mapping = KeyedStream.of(localAndRemotes.all()).map(mapper).collectTo(HashBiMap::create);

            Map<T1, ExecutorService> newExecutors = KeyedStream.stream(executors)
                    .mapKeys(mapping::get)
                    .collectToMap();

            return new UseCurrentThreadForLocalExecution<>(localAndRemotes.map(mapping::get), newExecutors);
        }

        private <R extends PaxosResponse> Result<T, R> executeLocally(Function<T, R> function) {
            try {
                R localResult = function.apply(localAndRemotes.local());
                return Results.success(localAndRemotes.local(), localResult);
            } catch (Exception e) {
                log.error("received error whilst trying to run local function", e);
                return Results.failure(e);
            }
        }
    }

    private interface PollOnlyCompletionService<T, R> {
        Optional<ListenableFuture<Map.Entry<T, R>>> poll(Duration timeout) throws InterruptedException;

        static <T, R> PollOnlyCompletionService<T, R> fromMultiplexing(MultiplexingCompletionService<T, R> multiplexing) {
            return waitTime -> Optional.ofNullable(multiplexing.poll(waitTime.toMillis(), TimeUnit.MILLISECONDS));
        }

        static <T, R> PollOnlyCompletionService<T, R> fromBlockingQueue(
                BlockingQueue<ListenableFuture<Map.Entry<T, R>>> blockingQueue) {
            return waitTime -> Optional.ofNullable(blockingQueue.poll(waitTime.toMillis(), TimeUnit.MILLISECONDS));
        }
    }

    private static final class AsyncExecutionEnvironment<S> implements PaxosExecutionEnvironment<S> {

        private static final Logger log = LoggerFactory.getLogger(AsyncExecutionEnvironment.class);

        private final List<S> services;

        private AsyncExecutionEnvironment(List<S> services) {
            this.services = services;
        }

        @Override
        public int numberOfServices() {
            return services.size();
        }

        @Override
        public <R extends PaxosResponse> ExecutionContext<S, R> execute(Function<S, R> function) {
            log.error("Cannot execute synchronous methods on an async environment as they block");
            RejectedExecutionException rejectedExecutionException = new RejectedExecutionException(
                    "Cannot execute synchronous methods on an async environment as they block");
            Queue<Result<S, R>> precomputedFailures = services.stream()
                    .<Result<S, R>>map(service -> Results.failure(rejectedExecutionException))
                    .collect(Collectors.toCollection(Queues::newArrayDeque));
            return new ExecutionContextImpl<>(
                    precomputedFailures,
                    PollOnlyCompletionService.fromBlockingQueue(Queues.newArrayBlockingQueue(0)),
                    ImmutableList.of());
        }

        public <R extends PaxosResponse> ExecutionContext<S, R> executeAsync(
                AsyncFunction<S, R> asyncFunction) {
            BlockingQueue<ListenableFuture<Map.Entry<S, R>>> blockingQueue = new LinkedBlockingQueue<>();
            List<ListenableFuture<Map.Entry<S, R>>> responseFutures = services.stream()
                    .map(service -> callService(service, asyncFunction))
                    .collect(Collectors.toList());

            responseFutures.forEach(responseFuture -> responseFuture.addListener(
                    () -> blockingQueue.add(responseFuture),
                    MoreExecutors.directExecutor()));

            return new ExecutionContextImpl<>(
                    Queues.newArrayDeque(),
                    PollOnlyCompletionService.fromBlockingQueue(blockingQueue),
                    responseFutures);
        }

        private <R extends PaxosResponse> ListenableFuture<Map.Entry<S, R>> callService(
                S service,
                AsyncFunction<S, R> function) {
            return Futures.transform(
                    AtlasFutures.call(function, service),
                    result -> Maps.immutableEntry(service, result),
                    MoreExecutors.directExecutor());
        }

        @Override
        public <T> PaxosExecutionEnvironment<T> map(Function<S, T> mapper) {
            return new AsyncExecutionEnvironment<>(services.stream().map(mapper).collect(Collectors.toList()));
        }
    }
    private static final class ExecutionContextImpl<T, R extends PaxosResponse> implements ExecutionContext<T, R> {

        // used to cancel outstanding requests after we have already achieved a quorum or otherwise finished collecting
        // responses
        private static final ScheduledExecutorService CANCELLATION_EXECUTOR = PTExecutors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory("paxos-quorum-checker-canceller", true));

        private static final Duration OUTSTANDING_REQUEST_CANCELLATION_TIMEOUT = Duration.ofMillis(2);
        private final Queue<PaxosExecutionEnvironment.Result<T, R>> existingResults;
        private final PollOnlyCompletionService<T, R> responseCompletionService;

        private final List<ListenableFuture<Map.Entry<T, R>>> responseFutures;

        private ExecutionContextImpl(
                Queue<PaxosExecutionEnvironment.Result<T, R>> existingResults,
                PollOnlyCompletionService<T, R> responseCompletionService,
                List<ListenableFuture<Map.Entry<T, R>>> responseFutures) {
            this.existingResults = existingResults;
            this.responseCompletionService = responseCompletionService;
            this.responseFutures = responseFutures;
        }

        @Override
        public PaxosExecutionEnvironment.Result<T, R> awaitNextResult(Instant deadline) throws InterruptedException {
            Instant now = Instant.now();
            if (now.isAfter(deadline)) {
                return Results.deadlineExceeded();
            }

            if (existingResults.peek() != null) {
                return existingResults.poll();
            }

            Duration waitTime = Duration.between(now, deadline);
            Optional<ListenableFuture<Map.Entry<T, R>>> responseFuture = responseCompletionService.poll(waitTime);

            if (!responseFuture.isPresent()) {
                return Results.deadlineExceeded();
            }

            try {
                Map.Entry<T, R> done = Futures.getDone(responseFuture.get());
                return Results.success(done.getKey(), done.getValue());
            } catch (ExecutionException e) {
                return Results.failure(e.getCause());
            }
        }

        @Override
        public ExecutionContext<T, R> withExistingResults(
                List<PaxosExecutionEnvironment.Result<T, R>> existingResults) {
            this.existingResults.addAll(existingResults);
            return this;
        }

        @Override
        public void cancel() {
            cancelOutstandingRequestsAfterTimeout();
        }
        private void cancelOutstandingRequestsAfterTimeout() {
            boolean areAllRequestsComplete = responseFutures.stream().allMatch(Future::isDone);
            if (areAllRequestsComplete) {
                return;
            }

            // give the remaining tasks some time to finish before interrupting them; this reduces overhead of
            // throwing exceptions
            CANCELLATION_EXECUTOR.schedule(
                    () -> responseFutures.forEach(future -> future.cancel(true)),
                    OUTSTANDING_REQUEST_CANCELLATION_TIMEOUT.toMillis(),
                    TimeUnit.MILLISECONDS);
        }

    }
}
