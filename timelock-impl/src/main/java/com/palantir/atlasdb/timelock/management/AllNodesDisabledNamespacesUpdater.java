/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.management;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesUpdaterService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SingleNodeDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SingleNodeReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulReenableNamespacesResponse;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponsesWithRemote;
import com.palantir.paxos.WrappedPaxosResponse;
import com.palantir.tokens.auth.AuthHeader;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class AllNodesDisabledNamespacesUpdater {
    private static final SafeLogger log = SafeLoggerFactory.get(AllNodesDisabledNamespacesUpdater.class);

    private final AuthHeader authHeader;
    private final ImmutableList<DisabledNamespacesUpdaterService> remoteUpdaters;
    private final Map<DisabledNamespacesUpdaterService, CheckedRejectionExecutorService> remoteExecutors;
    private final TimelockNamespaces localUpdater;
    private final Supplier<UUID> lockIdSupplier;
    private final int clusterSize;

    @VisibleForTesting
    AllNodesDisabledNamespacesUpdater(
            AuthHeader authHeader,
            ImmutableList<DisabledNamespacesUpdaterService> remoteUpdaters,
            Map<DisabledNamespacesUpdaterService, CheckedRejectionExecutorService> remoteExecutors,
            TimelockNamespaces localUpdater,
            Supplier<UUID> lockIdSupplier) {
        this.authHeader = authHeader;
        this.remoteUpdaters = remoteUpdaters;
        this.remoteExecutors = remoteExecutors;
        this.localUpdater = localUpdater;
        this.lockIdSupplier = lockIdSupplier;
        this.clusterSize = remoteUpdaters.size() + 1;
    }

    public static AllNodesDisabledNamespacesUpdater create(
            AuthHeader authHeader,
            ImmutableList<DisabledNamespacesUpdaterService> updaters,
            Map<DisabledNamespacesUpdaterService, CheckedRejectionExecutorService> executors,
            TimelockNamespaces localUpdater) {
        return new AllNodesDisabledNamespacesUpdater(authHeader, updaters, executors, localUpdater, UUID::randomUUID);
    }

    public DisableNamespacesResponse disableOnAllNodes(Set<Namespace> namespaces) {
        if (anyNodeIsUnreachable()) {
            log.error(
                    "Failed to reach all remote nodes. Not disabling any namespaces",
                    SafeArg.of("namespaces", namespaces));
            return DisableNamespacesResponse.unsuccessful(
                    UnsuccessfulDisableNamespacesResponse.builder().build());
        }

        UUID lockId = lockIdSupplier.get();
        List<SingleNodeDisableNamespacesResponse> responses = disableNamespacesOnAllNodes(namespaces, lockId);
        if (disableWasSuccessfulOnAllNodes(responses)) {
            return DisableNamespacesResponse.successful(SuccessfulDisableNamespacesResponse.of(lockId));
        }

        Set<Namespace> consistentlyDisabledNamespaces = getConsistentlyDisabledNamespaces(responses);
        if (!consistentlyDisabledNamespaces.isEmpty()) {
            log.error(
                    "Failed to disable all namespaces, because some namespace was consistently disabled. This implies"
                            + " that this namespace is already being restored. If that is the case, please either wait for"
                            + " that restore to complete, or kick off a restore without that namespace",
                    SafeArg.of("disabledNamespaces", consistentlyDisabledNamespaces));
            return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.builder()
                    .consistentlyDisabledNamespaces(consistentlyDisabledNamespaces)
                    .build());
        }

        // If no namespaces were consistently disabled, we should roll back our request,
        // to avoid leaving ourselves in an inconsistent state.
        boolean rollbackSuccess = attemptReEnableOnAllNodes(namespaces, lockId, responses.size());
        if (rollbackSuccess) {
            log.error(
                    "Failed to disable all namespaces. However, we successfully rolled back any partially disabled"
                            + " namespaces.",
                    SafeArg.of("namespaces", namespaces),
                    SafeArg.of("lockId", lockId));
            return DisableNamespacesResponse.unsuccessful(
                    UnsuccessfulDisableNamespacesResponse.builder().build());
        }

        log.error(
                "Failed to disable all namespaces, and we failed to roll back some namespaces."
                        + " These will need to be force-re-enabled in order to return Timelock to a consistent state.",
                SafeArg.of("namespaces", namespaces),
                SafeArg.of("lockId", lockId));
        return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.builder()
                .consistentlyDisabledNamespaces(consistentlyDisabledNamespaces)
                .partiallyDisabledNamespaces(namespaces)
                .build());
    }

    public ReenableNamespacesResponse reEnableOnAllNodes(ReenableNamespacesRequest request) {
        Set<Namespace> namespaces = request.getNamespaces();

        if (anyNodeIsUnreachable()) {
            log.error(
                    "Failed to reach all remote nodes. Not re-enabling any namespaces",
                    SafeArg.of("namespaces", namespaces));
            return ReenableNamespacesResponse.unsuccessful(
                    UnsuccessfulReenableNamespacesResponse.builder().build());
        }

        List<SingleNodeReenableNamespacesResponse> responses = reEnableNamespacesOnAllNodes(request);
        if (reEnableWasSuccessfulOnAllNodes(responses)) {
            return ReenableNamespacesResponse.successful(SuccessfulReenableNamespacesResponse.of(true));
        }

        Set<Namespace> consistentlyLockedNamespaces = getConsistentlyLockedNamespaces(responses);
        if (!consistentlyLockedNamespaces.isEmpty()) {
            log.error(
                    "Failed to re-enable all namespaces, because some namespace was consistently disabled "
                            + "with the wrong lock ID. This implies that this namespace being restored by another"
                            + " process. If that is the case, please either wait for that restore to complete, "
                            + " or kick off a restore without that namespace",
                    SafeArg.of("lockedNamespaces", consistentlyLockedNamespaces));
            return ReenableNamespacesResponse.unsuccessful(UnsuccessfulReenableNamespacesResponse.builder()
                    .consistentlyLockedNamespaces(consistentlyLockedNamespaces)
                    .build());
        }

        UUID lockId = request.getLockId();
        boolean rollbackSuccess = attemptDisableOnAllNodes(namespaces, lockId, responses.size());
        if (rollbackSuccess) {
            log.error(
                    "Failed to re-enable namespaces. However, we successfully rolled back any partially re-enabled"
                            + " namespaces",
                    SafeArg.of("namespaces", namespaces),
                    SafeArg.of("lockId", lockId));
            return ReenableNamespacesResponse.unsuccessful(
                    UnsuccessfulReenableNamespacesResponse.builder().build());
        }

        log.error(
                "Failed to re-enable all namespaces, and we failed to roll back some partially re-enabled namespaces."
                        + " These will need to be force-re-enabled in order to return Timelock to a consistent state.",
                SafeArg.of("namespaces", namespaces),
                SafeArg.of("lockId", lockId));
        return ReenableNamespacesResponse.unsuccessful(UnsuccessfulReenableNamespacesResponse.builder()
                .partiallyLockedNamespaces(namespaces)
                .build());
    }

    // Ping
    private boolean anyNodeIsUnreachable() {
        return !isSuccessfulOnAllNodes(service -> new BooleanPaxosResponse(service.ping(authHeader)));
    }

    private boolean isSuccessfulOnAllNodes(Function<DisabledNamespacesUpdaterService, PaxosResponse> request) {
        Collection<PaxosResponse> remoteResponses =
                executeOnAllRemoteNodes(request).responses().values();
        return remoteResponses.size() == remoteUpdaters.size()
                && remoteResponses.stream().allMatch(PaxosResponse::isSuccessful);
    }

    // Disable
    private boolean attemptDisableOnAllNodes(Set<Namespace> namespaces, UUID lockId, int expectedResponseCount) {
        List<SingleNodeDisableNamespacesResponse> rollbackResponses = disableNamespacesOnAllNodes(namespaces, lockId);
        return disableWasSuccessfulOnAllNodes(rollbackResponses, expectedResponseCount);
    }

    private List<SingleNodeDisableNamespacesResponse> disableNamespacesOnAllNodes(
            Set<Namespace> namespaces, UUID lockId) {
        DisableNamespacesRequest request = DisableNamespacesRequest.of(namespaces, lockId);
        Function<DisabledNamespacesUpdaterService, SingleNodeDisableNamespacesResponse> update =
                service -> service.disable(authHeader, request);
        List<SingleNodeDisableNamespacesResponse> responses = attemptOnAllRemoteNodes(update);

        // TODO(gs): only actually disable locally if successful; otherwise, just validate
        SingleNodeDisableNamespacesResponse localResponse = localUpdater.disable(request);
        responses.add(localResponse);

        return responses;
    }

    private boolean disableWasSuccessfulOnAllNodes(List<SingleNodeDisableNamespacesResponse> responses) {
        return disableWasSuccessfulOnAllNodes(responses, clusterSize);
    }

    private static boolean disableWasSuccessfulOnAllNodes(
            List<SingleNodeDisableNamespacesResponse> responses, int expectedResponseCount) {
        if (responses.size() < expectedResponseCount) {
            log.error(
                    "Failed to reach some node(s) during disable operation",
                    SafeArg.of("responseCount", responses.size()),
                    SafeArg.of("expectedResponseCount", expectedResponseCount));
            return false;
        }

        return responses.stream().allMatch(SingleNodeDisableNamespacesResponse::getWasSuccessful);
    }

    // ReEnable
    private boolean attemptReEnableOnAllNodes(Set<Namespace> namespaces, UUID lockId, int expectedResponseCount) {
        ReenableNamespacesRequest rollbackRequest = ReenableNamespacesRequest.builder()
                .namespaces(namespaces)
                .lockId(lockId)
                .build();
        List<SingleNodeReenableNamespacesResponse> responses = reEnableNamespacesOnAllNodes(rollbackRequest);
        return reEnableWasSuccessfulOnAllNodes(responses, expectedResponseCount);
    }

    private List<SingleNodeReenableNamespacesResponse> reEnableNamespacesOnAllNodes(ReenableNamespacesRequest request) {
        Function<DisabledNamespacesUpdaterService, SingleNodeReenableNamespacesResponse> update =
                service -> service.reenable(authHeader, request);
        List<SingleNodeReenableNamespacesResponse> responses = attemptOnAllRemoteNodes(update);
        SingleNodeReenableNamespacesResponse localResponse = localUpdater.reEnable(request);
        responses.add(localResponse);
        return responses;
    }

    private boolean reEnableWasSuccessfulOnAllNodes(List<SingleNodeReenableNamespacesResponse> responses) {
        return reEnableWasSuccessfulOnAllNodes(responses, clusterSize);
    }

    private static boolean reEnableWasSuccessfulOnAllNodes(
            List<SingleNodeReenableNamespacesResponse> responses, int expectedResponseCount) {
        if (responses.size() < expectedResponseCount) {
            log.error(
                    "Failed to reach some node(s) during reEnable operation",
                    SafeArg.of("responseCount", responses.size()),
                    SafeArg.of("expectedResponseCount", expectedResponseCount));
            return false;
        }

        return responses.stream().allMatch(SingleNodeReenableNamespacesResponse::getWasSuccessful);
    }

    // Failure analysis
    private static Set<Namespace> getConsistentlyLockedNamespaces(
            List<SingleNodeReenableNamespacesResponse> responses) {
        return getConsistentFailures(getNamespacesByReEnableFailureCount(responses), responses.size());
    }

    private static Set<Namespace> getConsistentlyDisabledNamespaces(
            List<SingleNodeDisableNamespacesResponse> responses) {
        return getConsistentFailures(getNamespacesByDisableFailureCount(responses), responses.size());
    }

    private static Set<Namespace> getConsistentFailures(Map<Namespace, Integer> failedNamespaces, int responseCount) {
        return KeyedStream.stream(failedNamespaces)
                .filter(val -> responseCount == val)
                .keys()
                .collect(Collectors.toSet());
    }

    private static Map<Namespace, Integer> getNamespacesByReEnableFailureCount(
            List<SingleNodeReenableNamespacesResponse> responses) {
        Map<Namespace, Integer> failedNamespaces = new HashMap<>();
        for (SingleNodeReenableNamespacesResponse response : responses) {
            // TODO(gs): consider lock ID
            Set<Namespace> lockedNamespaces = response.getLockedNamespaces().keySet();
            lockedNamespaces.forEach(ns -> failedNamespaces.merge(ns, 1, Integer::sum));
        }
        return failedNamespaces;
    }

    private static Map<Namespace, Integer> getNamespacesByDisableFailureCount(
            List<SingleNodeDisableNamespacesResponse> responses) {
        Map<Namespace, Integer> failedNamespaces = new HashMap<>();
        for (SingleNodeDisableNamespacesResponse response : responses) {
            Set<Namespace> disabledNamespaces = response.getLockedNamespaces().keySet();
            disabledNamespaces.forEach(ns -> failedNamespaces.merge(ns, 1, Integer::sum));
        }
        return failedNamespaces;
    }

    // Execution
    private <T> List<T> attemptOnAllRemoteNodes(Function<DisabledNamespacesUpdaterService, T> request) {
        Function<DisabledNamespacesUpdaterService, WrappedPaxosResponse<T>> composedFunction =
                service -> new WrappedPaxosResponse<>(request.apply(service));
        return executeOnAllRemoteNodes(composedFunction).responses().values().stream()
                .map(WrappedPaxosResponse::getResponse)
                .collect(Collectors.toList());
    }

    private <T extends PaxosResponse>
            PaxosResponsesWithRemote<DisabledNamespacesUpdaterService, T> executeOnAllRemoteNodes(
                    Function<DisabledNamespacesUpdaterService, T> request) {
        return PaxosQuorumChecker.collectQuorumResponses(
                remoteUpdaters, request, remoteUpdaters.size(), remoteExecutors, Duration.ofSeconds(5L), false);
    }
}
