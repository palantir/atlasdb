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

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.logsafe.Preconditions;

@SuppressWarnings("MethodTypeParameterName")
public final class PaxosQuorumChecker {

    public static final Duration DEFAULT_REMOTE_REQUESTS_TIMEOUT = Duration.ofSeconds(5);
    private static final Logger log = LoggerFactory.getLogger(PaxosQuorumChecker.class);
    private static final String PAXOS_MESSAGE_ERROR =
            "We encountered an error while trying to request an acknowledgement from another paxos node."
                    + " This could mean the node is down, or we cannot connect to it for some other reason.";

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
    public static <SERVICE, RESPONSE extends PaxosResponse> PaxosResponsesWithRemote<SERVICE, RESPONSE>
            collectQuorumResponses(
            ImmutableList<SERVICE> remotes,
            Function<SERVICE, RESPONSE> request,
            int quorumSize,
            Map<? extends SERVICE, ExecutorService> executors,
            Duration remoteRequestTimeout,
            boolean cancelRemainingCalls) {
        Preconditions.checkState(executors.keySet().equals(Sets.newHashSet(remotes)),
                "Each remote should have an executor.");
        return collectResponses(
                PaxosExecutionEnvironments.threadPerService(remotes, executors),
                request,
                quorumSize,
                remoteRequestTimeout,
                quorumShortcutPredicate(quorumSize),
                cancelRemainingCalls);
    }

    public static <SERVICE, RESPONSE extends PaxosResponse> PaxosResponsesWithRemote<SERVICE, RESPONSE>
            collectQuorumResponses(
            PaxosExecutionEnvironment<SERVICE> executionEnvironment,
            Function<SERVICE, RESPONSE> request,
            int quorumSize,
            Duration remoteRequestTimeout,
            boolean cancelRemainingCalls) {
        return collectResponses(
                executionEnvironment,
                request,
                quorumSize,
                remoteRequestTimeout,
                quorumShortcutPredicate(quorumSize),
                cancelRemainingCalls);
    }

    private static <SERVICE, RESPONSE> Predicate<InProgressResponseState<SERVICE, RESPONSE>>
            quorumShortcutPredicate(int quorum) {
        return currentState -> currentState.successes() >= quorum
                || currentState.failures() > currentState.totalRequests() - quorum;
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
    public static <SERVICE, RESPONSE extends PaxosResponse> PaxosResponses<RESPONSE>
            collectAsManyResponsesAsPossible(
            ImmutableList<SERVICE> remotes,
            Function<SERVICE, RESPONSE> request,
            ExecutorService executorService,
            Duration remoteRequestTimeout,
            boolean cancelRemainingCalls) {
        return collectResponses(
                PaxosExecutionEnvironments.threadPerService(
                        remotes,
                        mapToSingleExecutorService(remotes, executorService)),
                request,
                remotes.size(),
                remoteRequestTimeout,
                $ -> false,
                cancelRemainingCalls).withoutRemotes();
    }

    public static <SERVICE, RESPONSE extends PaxosResponse> PaxosResponsesWithRemote<SERVICE, RESPONSE> collectUntil(
            ImmutableList<SERVICE> remotes,
            Function<SERVICE, RESPONSE> request,
            Map<SERVICE, ExecutorService> executors,
            Duration remoteRequestTimeout,
            Predicate<InProgressResponseState<SERVICE, RESPONSE>> predicate,
            boolean cancelRemainingCalls) {
        return collectResponses(
                PaxosExecutionEnvironments.threadPerService(remotes, executors),
                request,
                remotes.size(),
                remoteRequestTimeout,
                predicate,
                cancelRemainingCalls);
    }

    private static <SERVICE> Map<SERVICE, ExecutorService> mapToSingleExecutorService(
            Collection<SERVICE> remotes,
            ExecutorService executorService) {
        return remotes.stream().collect(Collectors.toMap(remote -> remote, unused -> executorService));
    }

    /**
     * Collects a list of responses from remote services.
     * This method may short-circuit depending on the {@code shouldSkipNextRequest} predicate parameter and cancels
     * pending requests once the predicate is satisfied.
     *
     * @param executionEnvironment environment in which to run the request
     * @param request the request to make on each of the remote endpoints
     * @param quorumSize number of acknowledge requests after termination
     * @param shouldSkipNextRequest whether or not the next request should be skipped
     * @param cancelRemainingCalls whether or not to cancel in-progress calls once we've received enough responses
     * @return a list of responses
     */
    private static <SERVICE, RESPONSE extends PaxosResponse> PaxosResponsesWithRemote<SERVICE, RESPONSE>
            collectResponses(
            PaxosExecutionEnvironment<SERVICE> executionEnvironment,
            Function<SERVICE, RESPONSE> request,
            int quorumSize,
            Duration remoteRequestTimeout,
            Predicate<InProgressResponseState<SERVICE, RESPONSE>> shouldSkipNextRequest,
            boolean cancelRemainingCalls) {
        PaxosResponseAccumulator<SERVICE, RESPONSE> receivedResponses = PaxosResponseAccumulator.newResponse(
                executionEnvironment.numberOfServices(),
                quorumSize,
                shouldSkipNextRequest);

        PaxosExecutionEnvironment.ExecutionContext<SERVICE, RESPONSE> context = executionEnvironment.execute(request);

        List<Throwable> encounteredErrors = Lists.newArrayList();
        boolean interrupted = false;
        try {
            Instant deadline = Instant.now().plus(remoteRequestTimeout);
            while (receivedResponses.hasMoreRequests() && receivedResponses.shouldProcessNextRequest()) {
                PaxosExecutionEnvironment.Result<SERVICE, RESPONSE> nextResult =
                        context.awaitNextResult(deadline);
                boolean deadlineExceeded = Results.caseOf(nextResult)
                        .success((service, response) -> {
                            receivedResponses.add(service, response);
                            return false;
                        })
                        .deadlineExceeded_(true)
                        .failure(e -> {
                            receivedResponses.markFailure();
                            encounteredErrors.add(e);
                            return false;
                        });
                if (deadlineExceeded) {
                    break;
                }
            }
        } catch (InterruptedException e) {
            log.warn("paxos request interrupted", e);
            interrupted = true;
        } finally {
            if (cancelRemainingCalls) {
                context.cancel();
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

}
