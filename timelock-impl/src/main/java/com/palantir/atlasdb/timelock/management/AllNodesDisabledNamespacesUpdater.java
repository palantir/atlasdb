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
import com.google.common.collect.Sets;
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
import java.util.Collection;
import java.util.HashSet;
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
        Set<DisableNamespacesResponse> responses = attemptOnAllNodes(update, successEvaluator);
        boolean updateSuccess = responses.stream().allMatch(successEvaluator::apply);

        Set<DisableNamespacesResponse> localResponses = new HashSet<>();
        if (updateSuccess) {
            DisableNamespacesResponse response = localUpdater.disable(request);
            localResponses.add(response);
            if (response.accept(successfulDisableVisitor())) {
                return response;
            }
            // else, roll back
        }

        // roll back in the event of failure
        // TODO(gs): should we just force re-enable all namespaces?
        Set<DisableNamespacesResponse> allResponses = Sets.union(responses, localResponses);
        Set<Namespace> failedNamespaces = allResponses.stream()
                .map(resp -> resp.accept(disableResponseVisitor(
                        _unused -> ImmutableSet.of(), UnsuccessfulDisableNamespacesResponse::getDisabledNamespaces)))
                .flatMap(Collection::stream)
                .map(capture -> (Namespace) capture)
                .collect(Collectors.toSet());

        // Don't try to rollback if there's nothing to roll back
        if (failedNamespaces.isEmpty()) {
            return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.of(ImmutableSet.of()));
        }

        Set<Namespace> disabledNamespaces = Sets.difference(namespaces, failedNamespaces);
        ReenableNamespacesRequest rollbackRequest = ReenableNamespacesRequest.builder()
                .namespaces(failedNamespaces)
                .lockId(lockId)
                .build();
        boolean rollbackSuccess = attemptOnAllNodes(service -> new BooleanPaxosResponse(
                service.reenable(authHeader, rollbackRequest).getWasSuccessful()));
        localUpdater.reEnable(rollbackRequest);
        if (rollbackSuccess) {
            log.error(
                    "Failed to disable all namespaces. However, we successfully rolled back any partially disabled namespaces.",
                    SafeArg.of("disabledNamespaces", disabledNamespaces),
                    SafeArg.of("rolledBackNamespaces", failedNamespaces),
                    SafeArg.of("lockId", lockId));
            return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.of(ImmutableSet.of()));
        }

        log.error(
                // TODO(gs): explain how
                "Failed to disable all namespaces, and we failed to roll back some partially disabled namespaces."
                        + " These will need to be force-re-enabled in order to return Timelock to a consistent state.",
                SafeArg.of("disabledNamespaces", disabledNamespaces),
                SafeArg.of("brokenNamespaces", failedNamespaces),
                SafeArg.of("lockId", lockId));
        return DisableNamespacesResponse.unsuccessful(UnsuccessfulDisableNamespacesResponse.of(failedNamespaces));
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
    private <RESPONSE> Set<RESPONSE> attemptOnAllNodes(
            Function<DisabledNamespacesUpdaterService, RESPONSE> request,
            Function<RESPONSE, Boolean> successEvaluator) {
        Set<RESPONSE> responses = Sets.newConcurrentHashSet();
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
