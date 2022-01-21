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
import com.google.common.collect.Lists;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesUpdaterService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.SuccessfulDisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.UnsuccessfulDisableNamespacesResponse;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.streams.KeyedStream;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponsesWithRemote;
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
        boolean pingSuccess = attemptOnAllNodes(service -> new BooleanPaxosResponse(service.ping(authHeader)));

        if (!pingSuccess) {
            log.error(
                    "Failed to reach all remote nodes. Not disabling any namespaces",
                    SafeArg.of("namespaces", namespaces));
            return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.of(ImmutableSet.of()));
        }

        UUID lockId = lockIdSupplier.get();
        DisableNamespacesRequest request = DisableNamespacesRequest.of(namespaces, lockId);
        Function<DisabledNamespacesUpdaterService, DisableNamespacesResponse> update =
                service -> service.disable(authHeader, request);
        Function<DisableNamespacesResponse, Boolean> successEvaluator =
                response -> response.accept(successfulDisableVisitor());
        List<DisableNamespacesResponse> responses = attemptOnAllNodes(update, successEvaluator);

        DisableNamespacesResponse localResponse = localUpdater.disable(request);
        responses.add(localResponse);
        boolean updateSuccess = responses.stream().allMatch(successEvaluator::apply);
        if (updateSuccess) {
            return localResponse;
        }

        // roll back in the event of failure
        DisableNamespacesResponse.Visitor<Set<Namespace>> disabledNamespacesFetcher = disableResponseVisitor(
                _unused -> ImmutableSet.of(), UnsuccessfulDisableNamespacesResponse::getDisabledNamespaces);
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
            return DisableNamespacesResponse.unsuccessful(
                    UnsuccessfulDisableNamespacesResponse.of(consistentlyDisabledNamespaces));
        }

        // If no namespaces were consistently disabled, we should roll back our request,
        // to avoid leaving ourselves in an inconsistent state.
        ReenableNamespacesRequest rollbackRequest = ReenableNamespacesRequest.builder()
                .namespaces(namespaces)
                .lockId(lockId)
                .build();
        boolean rollbackSuccess = attemptOnAllNodes(service -> new BooleanPaxosResponse(
                service.reenable(authHeader, rollbackRequest).getWasSuccessful()));
        localUpdater.reEnable(rollbackRequest);
        if (rollbackSuccess) {
            log.error(
                    "Failed to disable all namespaces. However, we successfully rolled back any partially disabled namespaces.",
                    SafeArg.of("namespaces", namespaces),
                    SafeArg.of("lockId", lockId));
            return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.of(ImmutableSet.of()));
        }

        log.error(
                // TODO(gs): explain how
                "Failed to disable all namespaces, and we failed to roll back some partially disabled namespaces."
                        + " These will need to be force-re-enabled in order to return Timelock to a consistent state.",
                SafeArg.of("namespaces", namespaces),
                SafeArg.of("lockId", lockId));
        // TODO(gs): better arg?
        return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.of(namespaces));
    }

    public ReenableNamespacesResponse reenableOnAllNodes(ReenableNamespacesRequest request) {
        boolean pingSuccess = attemptOnAllNodes(service -> new BooleanPaxosResponse(service.ping(authHeader)));

        if (!pingSuccess) {
            log.error(
                    "Failed to reach all remote nodes. Not re-enabling any namespaces",
                    SafeArg.of("namespaces", request.getNamespaces()));
            return ReenableNamespacesResponse.of(false, ImmutableSet.of());
        }

        Function<DisabledNamespacesUpdaterService, PaxosResponse> update = service ->
                new BooleanPaxosResponse(service.reenable(authHeader, request).getWasSuccessful());
        boolean updateSuccess = attemptOnAllNodes(update);

        if (updateSuccess) {
            // TODO(gs): fall back if fails
            return localUpdater.reEnable(request);
        }

        // if all else fails, then... what? force-disable with the same lock ID
        // TODO(gs): if _this_ fails, log something sensible so the user can fix themselves
        //      (safe to assume this is already monitored)
        // TODO(gS): should be force-reenable
        //        ReenableNamespacesRequest rollbackRequest = ReenableNamespacesRequest.builder()
        //                .namespaces(request.getNamespaces())
        //                .lockId(UUID.randomUUID())
        //                .build();
        //        boolean rollbackSuccess = attemptOnAllNodes(service -> new BooleanPaxosResponse(
        //                service.reenable(authHeader, rollbackRequest).getWasSuccessful()));
        //        if (rollbackSuccess) {
        //            return DisableNamespacesResponse.of(false, null);
        //        }
        //
        // TODO(gs): proper exception
        throw new SafeRuntimeException("ohno");
    }

    // TODO(gs): generify PaxosQuorumChecker
    private <RESPONSE> List<RESPONSE> attemptOnAllNodes(
            Function<DisabledNamespacesUpdaterService, RESPONSE> request,
            Function<RESPONSE, Boolean> successEvaluator) {
        // Add one to take into account the local update
        List<RESPONSE> responses = Lists.newArrayListWithCapacity(updaters.size() + 1);
        Function<DisabledNamespacesUpdaterService, PaxosResponse> composedFunction = service -> {
            RESPONSE response = request.apply(service);
            responses.add(response);
            return new BooleanPaxosResponse(successEvaluator.apply(response));
        };
        PaxosQuorumChecker.collectQuorumResponses(
                updaters, composedFunction, updaters.size(), executors, Duration.ofSeconds(5L), false);
        return responses;
    }

    private boolean attemptOnAllNodes(Function<DisabledNamespacesUpdaterService, PaxosResponse> request) {
        PaxosResponsesWithRemote<DisabledNamespacesUpdaterService, PaxosResponse> responses =
                PaxosQuorumChecker.collectQuorumResponses(
                        updaters, request, updaters.size(), executors, Duration.ofSeconds(5L), true);
        return responses.responses().values().stream().allMatch(PaxosResponse::isSuccessful);
    }

    private DisableNamespacesResponse.Visitor<Boolean> successfulDisableVisitor() {
        return disableResponseVisitor(_unused -> true, _unused -> false);
    }

    private <T> DisableNamespacesResponse.Visitor<T> disableResponseVisitor(
            Function<SuccessfulDisableNamespacesResponse, T> visitSuccessful,
            Function<UnsuccessfulDisableNamespacesResponse, T> visitUnsuccessful) {
        return new DisableNamespacesResponse.Visitor<T>() {
            @Override
            public T visitSuccessful(SuccessfulDisableNamespacesResponse value) {
                return visitSuccessful.apply(value);
            }

            @Override
            public T visitUnsuccessful(UnsuccessfulDisableNamespacesResponse value) {
                return visitUnsuccessful.apply(value);
            }

            @Override
            public T visitUnknown(String unknownType) {
                throw new SafeIllegalStateException(
                        "Unknown DisabledNamespacesResponse", SafeArg.of("responseType", unknownType));
            }
        };
    }
}
