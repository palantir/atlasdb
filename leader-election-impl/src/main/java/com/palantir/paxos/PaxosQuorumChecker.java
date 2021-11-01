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
package com.palantir.paxos;

import com.codahale.metrics.Meter;
import com.google.common.collect.ImmutableList;
import com.palantir.common.concurrent.CheckedRejectedExecutionException;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.concurrent.MultiplexingCompletionService;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@SuppressWarnings("MethodTypeParameterName")
public final class PaxosQuorumChecker {

    public static final Duration DEFAULT_REMOTE_REQUESTS_TIMEOUT = Duration.ofSeconds(5);
    private static final SafeLogger log = SafeLoggerFactory.get(PaxosQuorumChecker.class);
    private static final String PAXOS_MESSAGE_ERROR =
            "We encountered an error while trying to request an acknowledgement from another paxos node."
                    + " This could mean the node is down, or we cannot connect to it for some other reason.";

    // used to cancel outstanding reqeusts after we have already achieved a quorum or otherwise finished collecting
    // responses
    private static final ScheduledExecutorService CANCELLATION_EXECUTOR = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("paxos-quorum-checker-canceller", true));
    private static final long OUTSTANDING_REQUEST_CANCELLATION_TIMEOUT_MILLIS = 2;

    private static final Meter requestExecutionRejection = new Meter();
    private static final Meter cancelOutstandingRequestNoOp = new Meter();
    private static final Meter cancelOutstandingRequestSuccess = new Meter();

    private PaxosQuorumChecker() {
        // Private constructor. Disallow instantiation.
    }

    /**
     * Collects a list of responses from a quorum of remote services.
     * This method short-circuits if a quorum can no longer be obtained (if too many servers have sent nacks), and
     * cancels pending requests once a quorum has been obtained.
     *
     * @param remotes a list endpoints to make the remote call on
     * @param request the request to make on each of the remote endpoints
     * @param quorumSize number of acknowledge requests required to reach quorum
     * @param executors runs requests for a given remote on its own executor
     * @param remoteRequestTimeout timeout for the call
     * @param cancelRemainingCalls whether or not to cancel in progress calls after we've received enough responses
     * @return a list responses
     */
    public static <SERVICE, RESPONSE extends PaxosResponse>
            PaxosResponsesWithRemote<SERVICE, RESPONSE> collectQuorumResponses(
                    ImmutableList<SERVICE> remotes,
                    Function<SERVICE, RESPONSE> request,
                    int quorumSize,
                    Map<? extends SERVICE, CheckedRejectionExecutorService> executors,
                    Duration remoteRequestTimeout,
                    boolean cancelRemainingCalls) {
        Preconditions.checkState(
                executors.keySet().equals(new HashSet<>(remotes)), "Each remote should have an executor.");
        return collectResponses(
                remotes,
                request,
                quorumSize,
                remoteRequestTimeout,
                quorumShortcutPredicate(quorumSize),
                cancelRemainingCalls,
                MultiplexingCompletionService.createFromCheckedExecutors(executors));
    }

    private static <SERVICE, RESPONSE> Predicate<InProgressResponseState<SERVICE, RESPONSE>> quorumShortcutPredicate(
            int quorum) {
        return currentState ->
                currentState.successes() >= quorum || currentState.failures() > currentState.totalRequests() - quorum;
    }

    /**
     * Collects as many responses as possible from remote services.
     * This method will continue even in the presence of nacks.
     *
     * @param remotes a list of endpoints to make the remote call on
     * @param request the request to make on each of the remote endpoints
     * @param executorService runs the requests
     * @param cancelRemainingCalls whether or not to cancel in progress calls after we've received enough responses
     * @return a list of responses
     */
    public static <SERVICE, RESPONSE extends PaxosResponse> PaxosResponses<RESPONSE> collectAsManyResponsesAsPossible(
            ImmutableList<SERVICE> remotes,
            Function<SERVICE, RESPONSE> request,
            ExecutorService executorService,
            Duration remoteRequestTimeout,
            boolean cancelRemainingCalls) {
        return collectResponses(
                        remotes,
                        request,
                        remotes.size(),
                        mapToSingleExecutorService(remotes, executorService),
                        remoteRequestTimeout,
                        _$ -> false,
                        cancelRemainingCalls)
                .withoutRemotes();
    }

    public static <SERVICE, RESPONSE extends PaxosResponse> PaxosResponsesWithRemote<SERVICE, RESPONSE> collectUntil(
            ImmutableList<SERVICE> remotes,
            Function<SERVICE, RESPONSE> request,
            Map<SERVICE, CheckedRejectionExecutorService> executors,
            Duration remoteRequestTimeout,
            Predicate<InProgressResponseState<SERVICE, RESPONSE>> predicate,
            boolean cancelRemainingCalls) {
        return collectResponses(
                remotes,
                request,
                remotes.size(),
                remoteRequestTimeout,
                predicate,
                cancelRemainingCalls,
                MultiplexingCompletionService.createFromCheckedExecutors(executors));
    }

    private static <SERVICE> Map<SERVICE, ExecutorService> mapToSingleExecutorService(
            Collection<SERVICE> remotes, ExecutorService executorService) {
        return remotes.stream().collect(Collectors.toMap(remote -> remote, _unused -> executorService));
    }

    /**
     * Collects a list of responses from remote services.
     * This method may short-circuit depending on the {@code shouldSkipNextRequest} predicate parameter and cancels
     * pending requests once the predicate is satisfied.
     *
     * @param remotes a list of endpoints to make the remote call on
     * @param request the request to make on each of the remote endpoints
     * @param quorumSize number of acknowledge requests after termination
     * @param executors run the requests
     * @param shouldSkipNextRequest whether or not the next request should be skipped
     * @param cancelRemainingCalls whether or not to cancel in-progress calls once we've received enough responses
     * @return a list of responses
     */
    private static <SERVICE, RESPONSE extends PaxosResponse>
            PaxosResponsesWithRemote<SERVICE, RESPONSE> collectResponses(
                    ImmutableList<SERVICE> remotes,
                    Function<SERVICE, RESPONSE> request,
                    int quorumSize,
                    Map<? extends SERVICE, ExecutorService> executors,
                    Duration remoteRequestTimeout,
                    Predicate<InProgressResponseState<SERVICE, RESPONSE>> shouldSkipNextRequest,
                    boolean cancelRemainingCalls) {
        MultiplexingCompletionService<SERVICE, RESPONSE> responseCompletionService =
                MultiplexingCompletionService.create(executors);

        return collectResponses(
                remotes,
                request,
                quorumSize,
                remoteRequestTimeout,
                shouldSkipNextRequest,
                cancelRemainingCalls,
                responseCompletionService);
    }

    private static <SERVICE, RESPONSE extends PaxosResponse>
            PaxosResponsesWithRemote<SERVICE, RESPONSE> collectResponses(
                    ImmutableList<SERVICE> remotes,
                    Function<SERVICE, RESPONSE> request,
                    int quorumSize,
                    Duration remoteRequestTimeout,
                    Predicate<InProgressResponseState<SERVICE, RESPONSE>> shouldSkipNextRequest,
                    boolean cancelRemainingCalls,
                    MultiplexingCompletionService<SERVICE, RESPONSE> responseCompletionService) {
        PaxosResponseAccumulator<SERVICE, RESPONSE> receivedResponses =
                PaxosResponseAccumulator.newResponse(remotes.size(), quorumSize, shouldSkipNextRequest);
        // kick off all the requests
        List<Future<Map.Entry<SERVICE, RESPONSE>>> allFutures = new ArrayList<>();
        for (SERVICE remote : remotes) {
            try {
                allFutures.add(responseCompletionService.submit(remote, () -> request.apply(remote)));
            } catch (CheckedRejectedExecutionException e) {
                requestExecutionRejection.mark();
                receivedResponses.markFailure();
                if (shouldLogDiagnosticInformation()) {
                    log.info("Quorum checker executor rejected task", e);
                    log.info(
                            "Rate of execution rejections: {}",
                            SafeArg.of("rate1m", requestExecutionRejection.getOneMinuteRate()));
                }
            }
        }

        List<Throwable> encounteredErrors = new ArrayList<>();
        boolean interrupted = false;
        try {
            long deadline = System.nanoTime() + remoteRequestTimeout.toNanos();
            while (receivedResponses.hasMoreRequests() && receivedResponses.shouldProcessNextRequest()) {
                try {
                    Future<Map.Entry<SERVICE, RESPONSE>> responseFuture =
                            responseCompletionService.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                    if (timedOut(responseFuture)) {
                        break;
                    }
                    receivedResponses.add(
                            responseFuture.get().getKey(), responseFuture.get().getValue());
                } catch (ExecutionException e) {
                    receivedResponses.markFailure();
                    encounteredErrors.add(e.getCause());
                }
            }
        } catch (InterruptedException e) {
            log.warn("paxos request interrupted", e);
            interrupted = true;
        } finally {
            if (cancelRemainingCalls) {
                cancelOutstandingRequestsAfterTimeout(allFutures);
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }

            if (!receivedResponses.hasQuorum()) {
                RuntimeException exceptionForSuppression = new RuntimeException("exception for suppresion");
                encounteredErrors.forEach(throwable -> {
                    throwable.addSuppressed(exceptionForSuppression);
                    log.warn(PAXOS_MESSAGE_ERROR, throwable);
                });
            }
        }
        return receivedResponses.collect();
    }

    private static boolean timedOut(Future<?> responseFuture) {
        return responseFuture == null;
    }

    private static <SERVICE, RESPONSE extends PaxosResponse> void cancelOutstandingRequestsAfterTimeout(
            List<Future<Map.Entry<SERVICE, RESPONSE>>> responseFutures) {

        boolean areAllRequestsComplete = responseFutures.stream().allMatch(Future::isDone);
        if (areAllRequestsComplete) {
            return;
        }

        // give the remaining tasks some time to finish before interrupting them; this reduces overhead of
        // throwing exceptions
        CANCELLATION_EXECUTOR.schedule(
                () -> {
                    for (Future<?> future : responseFutures) {
                        boolean isCanceled = future.cancel(true);
                        if (isCanceled) {
                            cancelOutstandingRequestSuccess.mark();
                        } else {
                            cancelOutstandingRequestNoOp.mark();
                        }
                    }
                },
                OUTSTANDING_REQUEST_CANCELLATION_TIMEOUT_MILLIS,
                TimeUnit.MILLISECONDS);

        if (log.isDebugEnabled() && shouldLogDiagnosticInformation()) {
            log.debug(
                    "Quorum checker canceled pending requests"
                            + ". Rate of successful cancellations: {}, rate of no-op cancellations: {}",
                    SafeArg.of("rateCancelled", cancelOutstandingRequestSuccess.getOneMinuteRate()),
                    SafeArg.of("rateNoOpCancellation", cancelOutstandingRequestNoOp.getOneMinuteRate()));
        }
    }

    private static boolean shouldLogDiagnosticInformation() {
        return Math.random() < 0.001;
    }
}
