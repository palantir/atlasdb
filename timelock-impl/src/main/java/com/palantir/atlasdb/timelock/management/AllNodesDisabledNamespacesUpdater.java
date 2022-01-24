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
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesUpdaterService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesResponse;
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
    private final ImmutableList<DisabledNamespacesUpdaterService> updaters;
    private final Map<DisabledNamespacesUpdaterService, CheckedRejectionExecutorService> executors;
    private final TimelockNamespaces localUpdater;
    private final Supplier<UUID> lockIdSupplier;

    @VisibleForTesting
    AllNodesDisabledNamespacesUpdater(
            AuthHeader authHeader,
            ImmutableList<DisabledNamespacesUpdaterService> updaters,
            Map<DisabledNamespacesUpdaterService, CheckedRejectionExecutorService> executors,
            TimelockNamespaces localUpdater,
            Supplier<UUID> lockIdSupplier) {
        this.authHeader = authHeader;
        this.updaters = updaters;
        this.executors = executors;
        this.localUpdater = localUpdater;
        this.lockIdSupplier = lockIdSupplier;
    }

    public static AllNodesDisabledNamespacesUpdater create(
            AuthHeader authHeader,
            ImmutableList<DisabledNamespacesUpdaterService> updaters,
            Map<DisabledNamespacesUpdaterService, CheckedRejectionExecutorService> executors,
            TimelockNamespaces localUpdater) {
        return new AllNodesDisabledNamespacesUpdater(authHeader, updaters, executors, localUpdater, UUID::randomUUID);
    }

    public DisableNamespacesResponse disableOnAllNodes(Set<Namespace> namespaces) {
        boolean pingSuccess = isSuccessfulOnAllNodes(service -> new BooleanPaxosResponse(service.ping(authHeader)));

        if (!pingSuccess) {
            log.error(
                    "Failed to reach all remote nodes. Not disabling any namespaces",
                    SafeArg.of("namespaces", namespaces));
            return DisableNamespacesResponse.unsuccessful(
                    UnsuccessfulDisableNamespacesResponse.builder().build());
        }

        UUID lockId = lockIdSupplier.get();
        DisableNamespacesRequest request = DisableNamespacesRequest.of(namespaces, lockId);
        Function<DisabledNamespacesUpdaterService, DisableNamespacesResponse> update =
                service -> service.disable(authHeader, request);
        Function<DisableNamespacesResponse, Boolean> successEvaluator =
                response -> response.accept(NamespaceUpdateVisitors.successfulDisableVisitor());

        List<DisableNamespacesResponse> responses = attemptOnAllNodes(update);

        DisableNamespacesResponse localResponse = localUpdater.disable(request);
        responses.add(localResponse);
        boolean updateSuccess = responses.stream().allMatch(successEvaluator::apply);
        if (updateSuccess) {
            return localResponse;
        }

        // roll back in the event of failure
        DisableNamespacesResponse.Visitor<Set<Namespace>> disabledNamespacesFetcher = NamespaceUpdateVisitors.disable(
                _unused -> ImmutableSet.of(), UnsuccessfulDisableNamespacesResponse::getConsistentlyDisabledNamespaces);
        Map<Namespace, Integer> failedNamespaces = new HashMap<>();
        for (DisableNamespacesResponse response : responses) {
            Set<Namespace> disabledNamespaces = response.accept(disabledNamespacesFetcher);
            disabledNamespaces.forEach(ns -> failedNamespaces.merge(ns, 1, Integer::sum));
        }

        // Find any consistently undisabled namespaces
        int numResponses = responses.size();
        Set<Namespace> consistentlyDisabledNamespaces = KeyedStream.stream(failedNamespaces)
                .filter(val -> numResponses == val)
                .keys()
                .collect(Collectors.toSet());
        if (!consistentlyDisabledNamespaces.isEmpty()) {
            log.error(
                    "Failed to disable all namespaces, because some namespace was consistently disabled. "
                            + "This implies that this namespace is already being restored. "
                            + "If that is the case, please either wait for that restore to complete, or kick off a restore "
                            + "without that namespace",
                    SafeArg.of("disabledNamespaces", consistentlyDisabledNamespaces));
            return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.builder()
                    .consistentlyDisabledNamespaces(consistentlyDisabledNamespaces)
                    .build());
        }

        // If no namespaces were consistently disabled, we should roll back our request,
        // to avoid leaving ourselves in an inconsistent state.
        ReenableNamespacesRequest rollbackRequest = ReenableNamespacesRequest.builder()
                .namespaces(namespaces)
                .lockId(lockId)
                .build();
        boolean rollbackSuccess =
                isSuccessfulOnAllNodes(service -> new BooleanPaxosResponse(service.reenable(authHeader, rollbackRequest)
                        .accept(NamespaceUpdateVisitors.successfulReEnableVisitor())));

        boolean localSuccess =
                localUpdater.reEnable(rollbackRequest).accept(NamespaceUpdateVisitors.successfulReEnableVisitor());
        if (rollbackSuccess && localSuccess) {
            log.error(
                    "Failed to disable all namespaces. However, we successfully rolled back any partially disabled namespaces.",
                    SafeArg.of("namespaces", namespaces),
                    SafeArg.of("lockId", lockId));
            return DisableNamespacesResponse.unsuccessful(
                    UnsuccessfulDisableNamespacesResponse.builder().build());
        }

        log.error(
                // TODO(gs): explain how
                "Failed to disable all namespaces, and we failed to roll back some namespaces."
                        + " These will need to be force-re-enabled in order to return Timelock to a consistent state.",
                SafeArg.of("namespaces", namespaces),
                SafeArg.of("lockId", lockId));

        return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.builder()
                .consistentlyDisabledNamespaces(consistentlyDisabledNamespaces)
                .partiallyDisabledNamespaces(namespaces)
                .build());
    }

    public ReenableNamespacesResponse reenableOnAllNodes(ReenableNamespacesRequest request) {
        Set<Namespace> namespaces = request.getNamespaces();
        UUID lockId = request.getLockId();

        boolean pingSuccess = isSuccessfulOnAllNodes(service -> new BooleanPaxosResponse(service.ping(authHeader)));
        if (!pingSuccess) {
            log.error(
                    "Failed to reach all remote nodes. Not re-enabling any namespaces",
                    SafeArg.of("namespaces", namespaces));
            return ReenableNamespacesResponse.unsuccessful(
                    UnsuccessfulReenableNamespacesResponse.builder().build());
        }

        Function<DisabledNamespacesUpdaterService, ReenableNamespacesResponse> update =
                service -> service.reenable(authHeader, request);
        Function<ReenableNamespacesResponse, Boolean> successEvaluator =
                res -> res.accept(NamespaceUpdateVisitors.successfulReEnableVisitor());
        List<ReenableNamespacesResponse> responses = attemptOnAllNodes(update);

        ReenableNamespacesResponse localResponse = localUpdater.reEnable(request);
        responses.add(localResponse);
        boolean updateSuccess = responses.stream().allMatch(successEvaluator::apply);
        if (updateSuccess) {
            return localResponse;
        }

        // Find any consistently locked namespaces
        ReenableNamespacesResponse.Visitor<Set<Namespace>> lockedNamespacesFetcher = NamespaceUpdateVisitors.reEnable(
                _unused -> ImmutableSet.of(), UnsuccessfulReenableNamespacesResponse::getConsistentlyLockedNamespaces);
        Map<Namespace, Integer> failedNamespaces = new HashMap<>();
        for (ReenableNamespacesResponse response : responses) {
            Set<Namespace> lockedNamespaces = response.accept(lockedNamespacesFetcher);
            lockedNamespaces.forEach(ns -> failedNamespaces.merge(ns, 1, Integer::sum));
        }

        int numResponses = responses.size();
        Set<Namespace> consistentlyLockedNamespaces = KeyedStream.stream(failedNamespaces)
                .filter(val -> numResponses == val)
                .keys()
                .collect(Collectors.toSet());
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

        // Fall back
        DisableNamespacesRequest rollbackRequest = DisableNamespacesRequest.of(namespaces, lockId);
        Function<DisabledNamespacesUpdaterService, DisableNamespacesResponse> rollback =
                service -> service.disable(authHeader, rollbackRequest);
        Function<DisableNamespacesResponse, Boolean> rollbackSuccessEvaluator =
                response -> response.accept(NamespaceUpdateVisitors.successfulDisableVisitor());
        List<DisableNamespacesResponse> rollbackResponses = attemptOnAllNodes(rollback);
        rollbackResponses.add(localUpdater.disable(rollbackRequest));
        if (rollbackResponses.stream().allMatch(rollbackSuccessEvaluator::apply)) {
            log.error(
                    "Failed to re-enable namespaces. However, we successfully rolled back any partially re-enabled namespaces",
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

    private <T> List<T> attemptOnAllNodes(Function<DisabledNamespacesUpdaterService, T> request) {
        Function<DisabledNamespacesUpdaterService, WrappedPaxosResponse<T>> composedFunction =
                service -> new WrappedPaxosResponse<>(request.apply(service));
        return executeOnAllNodes(composedFunction).responses().values().stream()
                .map(WrappedPaxosResponse::getResponse)
                .collect(Collectors.toList());
    }

    private boolean isSuccessfulOnAllNodes(Function<DisabledNamespacesUpdaterService, PaxosResponse> request) {
        return executeOnAllNodes(request).responses().values().stream().allMatch(PaxosResponse::isSuccessful);
    }

    private <T extends PaxosResponse> PaxosResponsesWithRemote<DisabledNamespacesUpdaterService, T> executeOnAllNodes(
            Function<DisabledNamespacesUpdaterService, T> request) {
        return PaxosQuorumChecker.collectQuorumResponses(
                updaters, request, updaters.size(), executors, Duration.ofSeconds(5L), false);
    }
}
