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
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.palantir.common.concurrent.MultiplexingCompletionService;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.paxos.PaxosExecutionEnvironment.ExecutionContext;

public final class PaxosExecutionEnvironments {

    private PaxosExecutionEnvironments() { }

    public static <S> PaxosExecutionEnvironment<S> threadPerService(
            List<S> services,
            Map<? extends S, ExecutorService> executors) {
        return new AllRequestsOnSeparateThreads<>(services, executors);
    }

    private static final class AllRequestsOnSeparateThreads<T> implements PaxosExecutionEnvironment<T> {

        private final List<T> remotes;
        private final Map<? extends T, ExecutorService> executors;

        private AllRequestsOnSeparateThreads(List<T> remotes, Map<? extends T, ExecutorService> executors) {
            this.remotes = remotes;
            this.executors = executors;
        }

        @Override
        public int numberOfServices() {
            return remotes.size();
        }

        @Override
        public <R extends PaxosResponse> ExecutionContext<T, R> execute(Function<T, R> request) {
            MultiplexingCompletionService<T, R> responseCompletionService =
                    MultiplexingCompletionService.create(executors);
            // kick off all the requests
            List<Future<Map.Entry<T, R>>> allFutures = Lists.newArrayList();
            Queue<PaxosExecutionEnvironment.Result<T, R>> submissionFailures = Lists.newLinkedList();
            for (T remote : remotes) {
                try {
                    allFutures.add(responseCompletionService.submit(remote, () -> request.apply(remote)));
                } catch (RejectedExecutionException e) {
                    submissionFailures.add(Results.failure(e));
                }
            }
            return new SynchronousExecutionContext<>(responseCompletionService, submissionFailures, allFutures);
        }
    }

    private static final class SynchronousExecutionContext<T, R extends PaxosResponse> implements ExecutionContext<T, R> {

        // used to cancel outstanding requests after we have already achieved a quorum or otherwise finished collecting
        // responses
        private static final ScheduledExecutorService CANCELLATION_EXECUTOR = PTExecutors
                .newSingleThreadScheduledExecutor(new NamedThreadFactory("paxos-quorum-checker-canceller", true));
        private static final Duration OUTSTANDING_REQUEST_CANCELLATION_TIMEOUT = Duration.ofMillis(2);

        private final MultiplexingCompletionService<T, R> responseCompletionService;
        private final Queue<PaxosExecutionEnvironment.Result<T, R>> submissionFailures;
        private final List<Future<Map.Entry<T, R>>> responseFutures;

        private SynchronousExecutionContext(
                MultiplexingCompletionService<T, R> responseCompletionService,
                Queue<PaxosExecutionEnvironment.Result<T, R>> submissionFailures,
                List<Future<Map.Entry<T, R>>> responseFutures) {
            this.responseCompletionService = responseCompletionService;
            this.submissionFailures = submissionFailures;
            this.responseFutures = responseFutures;
        }

        @Override
        public PaxosExecutionEnvironment.Result<T, R> awaitNextResult(Instant deadline) throws InterruptedException {
            if (submissionFailures.peek() != null) {
                return submissionFailures.poll();
            }

            Instant now = Instant.now();

            if (now.isAfter(deadline)) {
                return Results.deadlineExceeded();
            }

            Duration waitTime = Duration.between(now, deadline);
            Future<Map.Entry<T, R>> responseFuture =
                    responseCompletionService.poll(waitTime.toMillis(), TimeUnit.NANOSECONDS);

            if (responseFuture == null) {
                return Results.deadlineExceeded();
            }

            try {
                Map.Entry<T, R> done = Futures.getDone(responseFuture);
                return Results.success(done.getKey(), done.getValue());
            } catch (ExecutionException e) {
                return Results.failure(e.getCause());
            }
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
