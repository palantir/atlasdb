/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.paxos;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.common.concurrent.MultiplexingCompletionService;
import com.palantir.common.concurrent.NamedThreadFactory;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.logsafe.SafeArg;

@SuppressWarnings("MethodTypeParameterName")
public final class PaxosQuorumChecker {

    public static final int DEFAULT_REMOTE_REQUESTS_TIMEOUT_IN_SECONDS = 5;
    private static final Logger log = LoggerFactory.getLogger(PaxosQuorumChecker.class);
    private static final String PAXOS_MESSAGE_ERROR =
            "We encountered an error while trying to request an acknowledgement from another paxos node."
                    + " This could mean the node is down, or we cannot connect to it for some other reason.";

    // used to cancel outstanding reqeusts after we have already achieved a quorum or otherwise finished collecting
    // responses
    private static final ScheduledExecutorService CANCELLATION_EXECUTOR = PTExecutors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("paxos-quorum-checker-canceller", true));
    private static final long OUTSTANDING_REQUEST_CANCELLATION_TIMEOUT_MILLIS = 2;

    private static final Meter cancelOutstandingRequestNoOp =  new Meter();
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
     * @param executor runs the requests
     * @return a list responses
     */
    public static <SERVICE, RESPONSE extends PaxosResponse> List<RESPONSE> collectQuorumResponses(
            ImmutableList<SERVICE> remotes,
            final Function<SERVICE, RESPONSE> request,
            int quorumSize,
            ExecutorService executor,
            long remoteRequestTimeoutInSec) {
        return collectQuorumResponses(remotes, request, quorumSize, executor, remoteRequestTimeoutInSec, false);
    }

    public static <SERVICE, RESPONSE extends PaxosResponse> List<RESPONSE> collectQuorumResponses(
            ImmutableList<SERVICE> remotes,
            final Function<SERVICE, RESPONSE> request,
            int quorumSize,
            ExecutorService executor,
            long remoteRequestTimeoutInSec,
            boolean onlyLogOnQuorumFailure) {
        return collectResponses(
                remotes,
                request,
                quorumSize,
                MultiplexingCompletionService.createForSingleExecutor(remotes, executor),
                remoteRequestTimeoutInSec,
                onlyLogOnQuorumFailure,
                true);
    }

    public static <SERVICE, RESPONSE extends PaxosResponse> List<RESPONSE> collectQuorumResponses(
            ImmutableList<SERVICE> remotes,
            final Function<SERVICE, RESPONSE> request,
            int quorumSize,
            MultiplexingCompletionService<SERVICE, RESPONSE> executor,
            long remoteRequestTimeoutInSec,
            boolean onlyLogOnQuorumFailure) {
        return collectResponses(
                remotes, request, quorumSize, executor, remoteRequestTimeoutInSec, onlyLogOnQuorumFailure, true);
    }

    /**
     * Collects as many responses as possible from remote services.
     * This method will continue even in the presence of nacks.
     *
     * @param remotes a list of endpoints to make the remote call on
     * @param request the request to make on each of the remote endpoints
     * @param executor runs the requests
     * @return a list of responses
     */
    public static <SERVICE, RESPONSE extends PaxosResponse> List<RESPONSE> collectAsManyResponsesAsPossible(
            ImmutableList<SERVICE> remotes,
            final Function<SERVICE, RESPONSE> request,
            ExecutorService executor,
            long remoteRequestTimeoutInSec) {
        return collectResponses(
                remotes,
                request,
                remotes.size(),
                MultiplexingCompletionService.createForSingleExecutor(remotes, executor),
                remoteRequestTimeoutInSec,
                false,
                false);
    }

    public static <SERVICE, RESPONSE extends PaxosResponse> List<RESPONSE> collectAsManyResponsesAsPossible(
            ImmutableList<SERVICE> remotes,
            final Function<SERVICE, RESPONSE> request,
            MultiplexingCompletionService<SERVICE, RESPONSE> executor,
            long remoteRequestTimeoutInSec) {
        return collectResponses(remotes, request, remotes.size(), executor, remoteRequestTimeoutInSec, false, false);
    }

    /**
     * Collects a list of responses from remote services.
     * This method may short-circuit if a quorum can no longer be obtained (depending on the
     * shortcircuitIfQuorumImpossible parameter) and cancels pending requests once a quorum has been obtained.
     *
     * @param remotes a list of endpoints to make the remote call on
     * @param request the request to make on each of the remote endpoints
     * @param quorumSize number of acknowledge requests after termination
     * @param multiplexingExecutor runs the requests
     * @return a list of responses
     */
    private static <SERVICE, RESPONSE extends PaxosResponse> List<RESPONSE> collectResponses(
            ImmutableList<SERVICE> remotes,
            final Function<SERVICE, RESPONSE> request,
            int quorumSize,
            MultiplexingCompletionService<SERVICE, RESPONSE> multiplexingExecutor,
            long remoteRequestTimeoutInSec,
            boolean onlyLogOnQuorumFailure,
            boolean shortcircuitIfQuorumImpossible) {

        // kick off all the requests
        List<Future<RESPONSE>> allFutures = Lists.newArrayList();
        allFutures.addAll(multiplexingExecutor.execute(request).values());

        List<Throwable> toLog = Lists.newArrayList();
        boolean interrupted = false;
        List<RESPONSE> receivedResponses = new ArrayList<RESPONSE>();
        int acksRecieved = 0;
        int nacksRecieved = 0;

        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(remoteRequestTimeoutInSec);
            // handle responses
            while (acksRecieved < quorumSize) {
                try {
                    // check if quorum is impossible (nack quorum failure)
                    if (shortcircuitIfQuorumImpossible && nacksRecieved > remotes.size() - quorumSize) {
                        break;
                    }

                    Future<RESPONSE> responseFuture = multiplexingExecutor.poll(
                            deadline - System.nanoTime(),
                            TimeUnit.NANOSECONDS);
                    // check if out of responses (no quorum failure)
                    if (responseFuture == null) {
                        return receivedResponses;
                    }

                    // reject invalid or repeat promises
                    RESPONSE response = responseFuture.get();
                    if (response.isSuccessful()) {
                        acksRecieved++;
                    } else {
                        nacksRecieved++;
                    }

                    // record response
                    receivedResponses.add(response);
                } catch (InterruptedException e) {
                    log.warn("paxos request interrupted", e);
                    interrupted = true;
                    break;
                } catch (ExecutionException e) {
                    nacksRecieved++;
                    if (onlyLogOnQuorumFailure) {
                        toLog.add(e.getCause());
                    } else {
                        log.warn(PAXOS_MESSAGE_ERROR, e.getCause());
                    }
                }
            }

            // poll for extra completed futures
            Future<RESPONSE> future;
            while ((future = multiplexingExecutor.poll()) != null) {
                try {
                    receivedResponses.add(future.get());
                } catch (InterruptedException e) {
                    log.warn("paxos request interrupted", e);
                    interrupted = true;
                    break;
                } catch (ExecutionException e) {
                    log.warn(PAXOS_MESSAGE_ERROR, e.getCause());
                }
            }

        } finally {
            // cancel pending futures
            cancelOutstandingRequestsAfterTimeout(allFutures);

            // reset interrupted flag
            if (interrupted) {
                Thread.currentThread().interrupt();
            }

            if (onlyLogOnQuorumFailure && acksRecieved < quorumSize) {
                for (Throwable throwable : toLog) {
                    log.warn(PAXOS_MESSAGE_ERROR, throwable);
                }
            }
        }

        return receivedResponses;
    }

    private static <RESPONSE extends PaxosResponse> void cancelOutstandingRequestsAfterTimeout(
            List<Future<RESPONSE>> responseFutures) {
        boolean areAllRequestsComplete = Iterables.all(responseFutures, Future::isDone);
        if (areAllRequestsComplete) {
            return;
        }

        // give the remaining tasks some time to finish before interrupting them; this reduces overhead of
        // throwing exceptions
        CANCELLATION_EXECUTOR.schedule(() -> {
            for (Future<?> future : responseFutures) {
                boolean isCanceled = future.cancel(true);
                if (isCanceled) {
                    cancelOutstandingRequestSuccess.mark();
                } else {
                    cancelOutstandingRequestNoOp.mark();
                }
            }
        }, OUTSTANDING_REQUEST_CANCELLATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

        if (log.isDebugEnabled() && shouldLogCancellationStatus()) {
            log.debug("Quorum checker canceled pending requests"
                    + ". Rate of successful cancellations: {}, rate of no-op cancellations: {}",
                    SafeArg.of("rateCancelled", cancelOutstandingRequestSuccess.getOneMinuteRate()),
                    SafeArg.of("rateNoOpCancellation", cancelOutstandingRequestNoOp.getOneMinuteRate()));
        }
    }

    public static boolean hasQuorum(List<? extends PaxosResponse> responses, int quorumSize) {
        return Collections2.filter(responses, PaxosResponses.isSuccessfulPredicate()).size() >= quorumSize;
    }

    public static PaxosQuorumStatus getQuorumResult(List<PaxosResponse> responses, int quorumSize) {
        if (hasQuorum(responses, quorumSize)) {
            return PaxosQuorumStatus.QUORUM_AGREED;
        } else if (hasAnyDisagreements(responses)) {
            return PaxosQuorumStatus.SOME_DISAGREED;
        }

        return PaxosQuorumStatus.NO_QUORUM;
    }

    private static boolean hasAnyDisagreements(List<PaxosResponse> responses) {
        return responses.stream().anyMatch(response -> !response.isSuccessful());
    }

    private static boolean shouldLogCancellationStatus() {
        return Math.random() < 0.01;
    }
}
