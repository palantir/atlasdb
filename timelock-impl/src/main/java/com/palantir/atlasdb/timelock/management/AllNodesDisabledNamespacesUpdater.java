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
import com.google.common.collect.Sets;
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
import com.palantir.tokens.auth.AuthHeader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.immutables.value.Value;

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

    /**
     *  Attempts to disable the given set of namespaces on all nodes.
     *  First checks if all remote nodes are reachable - if so, it fails without attempting to disable on any node.
     *  Then attempts to disable the namespaces on all nodes, using a unique lock ID. This ID is returned to the caller,
     *  and must be supplied when re-enabling the namespaces.
     *  If, for some nodes, some namespaces are already disabled, then we do not overwrite the lock ID.
     *  In this case, we will attempt to roll back our operation, so that the set of disabled namespaces is left in
     *  the state in which we found it.
     *
     *  If some namespace is disabled on all nodes with the same lock ID, this implies that another restore is already
     *  running (or perhaps died).
     *
     *  If we fail to roll back for some node (e.g. it becomes unreachable), then we're left in an inconsistent state
     *  which will need manual remediation.
     */
    public DisableNamespacesResponse disableOnAllNodes(Set<Namespace> namespaces) {
        if (anyNodeIsUnreachable()) {
            return DisableNamespaceResponses.unsuccessfulDueToPingFailure(namespaces);
        }

        UUID lockId = lockIdSupplier.get();
        AllNodesUpdateResponse responses = disableNamespacesOnAllNodes(namespaces, lockId);
        if (updateWasSuccessfulOnAllNodes(responses.allResponses())) {
            return DisableNamespacesResponse.successful(SuccessfulDisableNamespacesResponse.of(lockId));
        }

        Set<Namespace> consistentlyLockedNamespaces = getConsistentFailures(responses.allResponses());
        Set<Namespace> partiallyLockedNamespaces = getPartialFailures(responses.allResponses());
        boolean updateFailedOnAllNodes = responses.remoteResponses().size() == remoteUpdaters.size()
                && responses.remoteResponses().values().stream().noneMatch(SingleNodeUpdateResponse::isSuccessful);
        if (updateFailedOnAllNodes) {
            return DisableNamespaceResponses.unsuccessfulDueToLockedNamespaces(
                    consistentlyLockedNamespaces, partiallyLockedNamespaces);
        }

        // If no namespaces were consistently disabled, we should roll back our request for those nodes where we
        // successfully disabled
        List<DisabledNamespacesUpdaterService> successfulNodes = KeyedStream.stream(responses.remoteResponses())
                .filter(PaxosResponse::isSuccessful)
                .keys()
                .collect(Collectors.toList());
        boolean rollbackSuccess = attemptReEnableOnNodes(namespaces, lockId, successfulNodes);
        if (rollbackSuccess) {
            return DisableNamespaceResponses.unsuccessfulButRolledBack(namespaces, lockId);
        }

        return DisableNamespaceResponses.unsuccessfulAndRollBackFailed(partiallyLockedNamespaces, lockId);
    }

    /**
     * Attempts to re-enable the provided namespaces on all nodes.
     * If some namespaces are locked with a different lock ID, then these namespaces will remain disabled, however
     * we will still re-enable those that were locked with the provided lock ID.
     */
    public ReenableNamespacesResponse reEnableOnAllNodes(ReenableNamespacesRequest request) {
        List<SingleNodeUpdateResponse> responses =
                reEnableNamespacesOnAllNodes(request).allResponses();
        if (updateWasSuccessfulOnAllNodes(responses)) {
            return ReenableNamespacesResponse.successful(SuccessfulReenableNamespacesResponse.of(true));
        }

        Set<Namespace> consistentlyLockedNamespaces = getConsistentFailures(responses);
        Set<Namespace> partialFailures = getPartialFailures(responses);
        if (!consistentlyLockedNamespaces.isEmpty()) {
            return ReEnableNamespaceResponses.unsuccessfulDueToConsistentlyLockedNamespaces(
                    consistentlyLockedNamespaces, partialFailures);
        }

        return ReEnableNamespaceResponses.unsuccessfulWithPartiallyLockedNamespaces(partialFailures);
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
    private AllNodesUpdateResponse disableNamespacesOnAllNodes(Set<Namespace> namespaces, UUID lockId) {
        DisableNamespacesRequest request = DisableNamespacesRequest.of(namespaces, lockId);
        Function<DisabledNamespacesUpdaterService, SingleNodeUpdateResponse> update =
                service -> service.disable(authHeader, request);
        Supplier<SingleNodeUpdateResponse> localUpdate = () -> localUpdater.disable(request);

        return attemptOnAllNodes(namespaces, lockId, update, localUpdate, false);
    }

    // ReEnable
    private boolean attemptReEnableOnNodes(
            Set<Namespace> namespaces, UUID lockId, List<DisabledNamespacesUpdaterService> successfulNodes) {
        ReenableNamespacesRequest request = ReenableNamespacesRequest.builder()
                .namespaces(namespaces)
                .lockId(lockId)
                .build();
        Collection<SingleNodeUpdateResponse> responses = reEnableNamespacesOnRemoteNodes(request, successfulNodes);
        return responses.stream().filter(SingleNodeUpdateResponse::isSuccessful).count() == successfulNodes.size();
    }

    private AllNodesUpdateResponse reEnableNamespacesOnAllNodes(ReenableNamespacesRequest request) {
        Set<Namespace> namespaces = request.getNamespaces();
        UUID lockId = request.getLockId();
        Function<DisabledNamespacesUpdaterService, SingleNodeUpdateResponse> update =
                service -> service.reenable(authHeader, request);
        Supplier<SingleNodeUpdateResponse> localUpdate = () -> localUpdater.reEnable(request);

        return attemptOnAllNodes(namespaces, lockId, update, localUpdate, true);
    }

    private Collection<SingleNodeUpdateResponse> reEnableNamespacesOnRemoteNodes(
            ReenableNamespacesRequest request, List<DisabledNamespacesUpdaterService> nodes) {
        Function<DisabledNamespacesUpdaterService, SingleNodeUpdateResponse> update =
                node -> node.reenable(authHeader, request);

        PaxosResponsesWithRemote<DisabledNamespacesUpdaterService, SingleNodeUpdateResponse> responses =
                PaxosQuorumChecker.collectAllResponses(
                        remoteUpdaters, update, remoteExecutors, Duration.ofSeconds(5L), false);
        return responses.responses().values();
    }

    // Update and analysis
    private AllNodesUpdateResponse attemptOnAllNodes(
            Set<Namespace> namespaces,
            UUID lockId,
            Function<DisabledNamespacesUpdaterService, SingleNodeUpdateResponse> update,
            Supplier<SingleNodeUpdateResponse> localUpdate,
            boolean alwaysAttemptOnLocalNode) {
        Map<DisabledNamespacesUpdaterService, SingleNodeUpdateResponse> remoteResponses =
                attemptOnAllRemoteNodes(update);
        SingleNodeUpdateResponse localResponse = updateLocallyOrCheckState(
                remoteResponses.values(), namespaces, lockId, localUpdate, alwaysAttemptOnLocalNode);
        return AllNodesUpdateResponse.builder()
                .remoteResponses(remoteResponses)
                .localResponse(localResponse)
                .build();
    }

    private SingleNodeUpdateResponse updateLocallyOrCheckState(
            Collection<SingleNodeUpdateResponse> responses,
            Set<Namespace> namespaces,
            UUID lockId,
            Supplier<SingleNodeUpdateResponse> localUpdate,
            boolean alwaysAttemptOnLocalNode) {
        if (alwaysAttemptOnLocalNode
                || (responses.stream().allMatch(SingleNodeUpdateResponse::isSuccessful)
                        && responses.size() == remoteUpdaters.size())) {
            return localUpdate.get();
        } else {
            Map<Namespace, UUID> incorrectlyLockedNamespaces =
                    localUpdater.getNamespacesLockedWithDifferentLockId(namespaces, lockId);
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

        return responses.stream().allMatch(SingleNodeUpdateResponse::isSuccessful);
    }

    private static Set<Namespace> getConsistentFailures(List<SingleNodeUpdateResponse> responses) {
        return getConsistentFailures(getNamespacesByFailureCount(responses), responses.size());
    }

    private static Set<Namespace> getConsistentFailures(
            Map<Namespace, UpdateFailureRecord> failedNamespaces, int responseCount) {
        return KeyedStream.stream(failedNamespaces)
                .filter(val -> val.isConsistent(responseCount))
                .keys()
                .collect(Collectors.toSet());
    }

    private static Set<Namespace> getPartialFailures(List<SingleNodeUpdateResponse> responses) {
        Map<Namespace, UpdateFailureRecord> namespacesByFailureCount = getNamespacesByFailureCount(responses);
        Set<Namespace> consistentFailures = getConsistentFailures(namespacesByFailureCount, responses.size());
        return Sets.difference(namespacesByFailureCount.keySet(), consistentFailures);
    }

    private static Map<Namespace, UpdateFailureRecord> getNamespacesByFailureCount(
            List<SingleNodeUpdateResponse> responses) {
        Map<Namespace, UpdateFailureRecord> failuresByNamespace = new HashMap<>();
        for (SingleNodeUpdateResponse response : responses) {
            KeyedStream.stream(response.lockedNamespaces()).forEach((namespace, lockId) -> {
                failuresByNamespace.merge(namespace, UpdateFailureRecord.of(lockId), UpdateFailureRecord::merge);
            });
        }
        return failuresByNamespace;
    }

    // Execution
    private Map<DisabledNamespacesUpdaterService, SingleNodeUpdateResponse> attemptOnAllRemoteNodes(
            Function<DisabledNamespacesUpdaterService, SingleNodeUpdateResponse> request) {
        return executeOnAllRemoteNodes(request).responses();
    }

    private <T extends PaxosResponse>
            PaxosResponsesWithRemote<DisabledNamespacesUpdaterService, T> executeOnAllRemoteNodes(
                    Function<DisabledNamespacesUpdaterService, T> request) {

        return PaxosQuorumChecker.collectAllResponses(
                remoteUpdaters, request, remoteExecutors, Duration.ofSeconds(5L), false);
    }

    @Value.Immutable
    interface AllNodesUpdateResponse {
        Map<DisabledNamespacesUpdaterService, SingleNodeUpdateResponse> remoteResponses();

        SingleNodeUpdateResponse localResponse();

        default List<SingleNodeUpdateResponse> allResponses() {
            List<SingleNodeUpdateResponse> responses =
                    new ArrayList<>(remoteResponses().values());
            responses.add(localResponse());
            return responses;
        }

        class Builder extends ImmutableAllNodesUpdateResponse.Builder {}

        static Builder builder() {
            return new Builder();
        }
    }
}
