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
import com.palantir.atlasdb.timelock.api.SingleNodeUpdateResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulReenableNamespacesResponse;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponsesWithRemote;
import com.palantir.paxos.SuccessfulPaxosResponse;
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
            return DisableNamespaceResponses.unsuccessfulDueToPingFailure(namespaces);
        }

        UUID lockId = lockIdSupplier.get();
        List<SingleNodeUpdateResponse> responses = disableNamespacesOnAllNodes(namespaces, lockId);
        if (updateWasSuccessfulOnAllNodes(responses)) {
            return DisableNamespacesResponse.successful(SuccessfulDisableNamespacesResponse.of(lockId));
        }

        Set<Namespace> consistentlyLockedNamespaces = getConsistentFailures(responses);
        if (!consistentlyLockedNamespaces.isEmpty()) {
            return DisableNamespaceResponses.unsuccessfulDueToConsistentlyLockedNamespaces(
                    consistentlyLockedNamespaces);
        }

        // If no namespaces were consistently disabled, we should roll back our request,
        // to avoid leaving ourselves in an inconsistent state.
        boolean rollbackSuccess = attemptReEnableOnAllNodes(namespaces, lockId, responses.size());
        if (rollbackSuccess) {
            return DisableNamespaceResponses.unsuccessfulButRolledBack(namespaces, lockId);
        }

        return DisableNamespaceResponses.unsuccessfulAndRollBackFailed(namespaces, lockId);
    }

    public ReenableNamespacesResponse reEnableOnAllNodes(ReenableNamespacesRequest request) {
        Set<Namespace> namespaces = request.getNamespaces();

        if (anyNodeIsUnreachable()) {
            return ReEnableNamespaceResponses.unsuccessfulDueToPingFailure(namespaces);
        }

        List<SingleNodeUpdateResponse> responses = reEnableNamespacesOnAllNodes(request);
        if (updateWasSuccessfulOnAllNodes(responses)) {
            return ReenableNamespacesResponse.successful(SuccessfulReenableNamespacesResponse.of(true));
        }

        Set<Namespace> consistentlyLockedNamespaces = getConsistentFailures(responses);
        if (!consistentlyLockedNamespaces.isEmpty()) {
            return ReEnableNamespaceResponses.unsuccessfulDueToConsistentlyLockedNamespaces(
                    consistentlyLockedNamespaces);
        }

        // TODO(gs): should only attempt to roll back locally?
        UUID lockId = request.getLockId();
        boolean rollbackSuccess = attemptDisableOnAllNodes(namespaces, lockId, responses.size());
        if (rollbackSuccess) {
            return ReEnableNamespaceResponses.unsuccessfulButRolledBack(namespaces, lockId);
        }

        return ReEnableNamespaceResponses.unsuccessfulAndRollBackFailed(namespaces, lockId);
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
        List<SingleNodeUpdateResponse> rollbackResponses = disableNamespacesOnAllNodes(namespaces, lockId);
        return updateWasSuccessfulOnAllNodes(rollbackResponses, expectedResponseCount);
    }

    private List<SingleNodeUpdateResponse> disableNamespacesOnAllNodes(Set<Namespace> namespaces, UUID lockId) {
        DisableNamespacesRequest request = DisableNamespacesRequest.of(namespaces, lockId);
        Function<DisabledNamespacesUpdaterService, SingleNodeUpdateResponse> update =
                service -> service.disable(authHeader, request);
        Supplier<SingleNodeUpdateResponse> localUpdate = () -> localUpdater.disable(request);

        return attemptOnAllNodes(namespaces, lockId, update, localUpdate);
    }

    // ReEnable
    private boolean attemptReEnableOnAllNodes(Set<Namespace> namespaces, UUID lockId, int expectedResponseCount) {
        ReenableNamespacesRequest request = ReenableNamespacesRequest.builder()
                .namespaces(namespaces)
                .lockId(lockId)
                .build();
        List<SingleNodeUpdateResponse> responses = reEnableNamespacesOnAllNodes(request);
        return updateWasSuccessfulOnAllNodes(responses, expectedResponseCount);
    }

    private List<SingleNodeUpdateResponse> reEnableNamespacesOnAllNodes(ReenableNamespacesRequest request) {
        Set<Namespace> namespaces = request.getNamespaces();
        UUID lockId = request.getLockId();
        Function<DisabledNamespacesUpdaterService, SingleNodeUpdateResponse> update =
                service -> service.reenable(authHeader, request);
        Supplier<SingleNodeUpdateResponse> localUpdate = () -> localUpdater.reEnable(request);

        return attemptOnAllNodes(namespaces, lockId, update, localUpdate);
    }

    // Update and analysis
    private List<SingleNodeUpdateResponse> attemptOnAllNodes(
            Set<Namespace> namespaces,
            UUID lockId,
            Function<DisabledNamespacesUpdaterService, SingleNodeUpdateResponse> update,
            Supplier<SingleNodeUpdateResponse> localUpdate) {
        List<SingleNodeUpdateResponse> responses = attemptOnAllRemoteNodes(update);
        SingleNodeUpdateResponse localResponse = updateLocallyOrCheckState(responses, namespaces, lockId, localUpdate);
        responses.add(localResponse);
        return responses;
    }

    private SingleNodeUpdateResponse updateLocallyOrCheckState(
            List<SingleNodeUpdateResponse> responses,
            Set<Namespace> namespaces,
            UUID lockId,
            Supplier<SingleNodeUpdateResponse> localUpdate) {
        if (responses.stream().allMatch(SingleNodeUpdateResponse::getWasSuccessful)
                && responses.size() == remoteUpdaters.size()) {
            return localUpdate.get();
        } else {
            Map<Namespace, UUID> incorrectlyLockedNamespaces =
                    localUpdater.getIncorrectlyLockedNamespaces(namespaces, lockId);
            return SingleNodeUpdateResponse.of(false, incorrectlyLockedNamespaces);
        }
    }

    private boolean updateWasSuccessfulOnAllNodes(List<SingleNodeUpdateResponse> responses) {
        return updateWasSuccessfulOnAllNodes(responses, clusterSize);
    }

    private static boolean updateWasSuccessfulOnAllNodes(
            List<SingleNodeUpdateResponse> responses, int expectedResponseCount) {
        if (responses.size() < expectedResponseCount) {
            log.error(
                    "Failed to reach some node(s) during update",
                    SafeArg.of("responseCount", responses.size()),
                    SafeArg.of("expectedResponseCount", expectedResponseCount));
            return false;
        }

        return responses.stream().allMatch(SingleNodeUpdateResponse::getWasSuccessful);
    }

    private static Set<Namespace> getConsistentFailures(List<SingleNodeUpdateResponse> responses) {
        return getConsistentFailures(getNamespacesByFailureCount(responses), responses.size());
    }

    private static Set<Namespace> getConsistentFailures(
            Map<Namespace, UpdateFailureRecord> failedNamespaces, int responseCount) {
        return KeyedStream.stream(failedNamespaces)
                .filter(val ->
                        val.failureCount() == responseCount && val.lockIds().size() == 1)
                .keys()
                .collect(Collectors.toSet());
    }

    private static Map<Namespace, UpdateFailureRecord> getNamespacesByFailureCount(
            List<SingleNodeUpdateResponse> responses) {
        Map<Namespace, UpdateFailureRecord> failuresByNamespace = new HashMap<>();
        for (SingleNodeUpdateResponse response : responses) {
            KeyedStream.stream(response.getLockedNamespaces()).forEach((namespace, lockId) -> {
                failuresByNamespace.merge(namespace, UpdateFailureRecord.of(lockId), UpdateFailureRecord::merge);
            });
        }
        return failuresByNamespace;
    }

    // Execution
    private <T> List<T> attemptOnAllRemoteNodes(Function<DisabledNamespacesUpdaterService, T> request) {
        Function<DisabledNamespacesUpdaterService, SuccessfulPaxosResponse<T>> composedFunction =
                service -> SuccessfulPaxosResponse.of(request.apply(service));
        return executeOnAllRemoteNodes(composedFunction).responses().values().stream()
                .map(SuccessfulPaxosResponse::getResponse)
                .collect(Collectors.toList());
    }

    private <T extends PaxosResponse>
            PaxosResponsesWithRemote<DisabledNamespacesUpdaterService, T> executeOnAllRemoteNodes(
                    Function<DisabledNamespacesUpdaterService, T> request) {
        return PaxosQuorumChecker.collectQuorumResponses(
                remoteUpdaters, request, remoteUpdaters.size(), remoteExecutors, Duration.ofSeconds(5L), false);
    }
}
