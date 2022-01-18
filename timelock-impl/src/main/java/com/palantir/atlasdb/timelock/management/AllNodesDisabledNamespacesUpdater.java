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

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.timelock.api.DisableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisableNamespacesResponse;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesUpdaterService;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesRequest;
import com.palantir.atlasdb.timelock.api.ReenableNamespacesResponse;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponsesWithRemote;
import com.palantir.tokens.auth.AuthHeader;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

public class AllNodesDisabledNamespacesUpdater {
    private final AuthHeader authHeader;
    private final ImmutableList<DisabledNamespacesUpdaterService> updaters;
    private final Map<DisabledNamespacesUpdaterService, CheckedRejectionExecutorService> executors;
    private final DisabledNamespaces localUpdater;

    public AllNodesDisabledNamespacesUpdater(
            AuthHeader authHeader,
            ImmutableList<DisabledNamespacesUpdaterService> updaters,
            Map<DisabledNamespacesUpdaterService, CheckedRejectionExecutorService> executors,
            DisabledNamespaces localUpdater) {
        this.authHeader = authHeader;
        this.updaters = updaters;
        this.executors = executors;
        this.localUpdater = localUpdater;
    }

    public DisableNamespacesResponse disableOnAllNodes(Set<Namespace> namespaces) {
        boolean pingSuccess = attemptOnAllNodes(service -> new BooleanPaxosResponse(service.ping(authHeader)));

        // TODO(gs): exception
        if (!pingSuccess) {
            // TODO(gs): no uuid
            return DisableNamespacesResponse.of(false, null);
        }

        UUID lockId = UUID.randomUUID();
        DisableNamespacesRequest request = DisableNamespacesRequest.of(namespaces, lockId);
        Function<DisabledNamespacesUpdaterService, PaxosResponse> update = service ->
                new BooleanPaxosResponse(service.disable(authHeader, request).getWasSuccessful());
        boolean updateSuccess = attemptOnAllNodes(update);

        if (updateSuccess) {
            // TODO(gs): fall back if fails
            return localUpdater.disable(request);
        }

        // if all else fails, do the opposite thing
        // TODO(gs): if _this_ fails, log something sensible so the user can fix themselves
        //      (safe to assume this is already monitored)
        // TODO(gS): should be force-reenable
        ReenableNamespacesRequest rollbackRequest = ReenableNamespacesRequest.builder()
                .namespaces(request.getNamespaces())
                .lockId(UUID.randomUUID())
                .build();
        boolean rollbackSuccess = attemptOnAllNodes(service -> new BooleanPaxosResponse(
                service.reenable(authHeader, rollbackRequest).getWasSuccessful()));
        if (rollbackSuccess) {
            return DisableNamespacesResponse.of(false, null);
        }

        // TODO(gs): proper exception
        throw new SafeRuntimeException("ohno");
    }

    public ReenableNamespacesResponse reenableOnAllNodes(ReenableNamespacesRequest request) {
        boolean pingSuccess = attemptOnAllNodes(service -> new BooleanPaxosResponse(service.ping(authHeader)));

        // TODO(gs): exception
        if (!pingSuccess) {
            return ReenableNamespacesResponse.of(false);
        }

        // TODO(gs): should have same UUID across all nodes
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

    private boolean attemptOnAllNodes(Function<DisabledNamespacesUpdaterService, PaxosResponse> request) {
        PaxosResponsesWithRemote<DisabledNamespacesUpdaterService, PaxosResponse> responses =
                PaxosQuorumChecker.collectQuorumResponses(
                        updaters, request, updaters.size(), executors, Duration.ofSeconds(5L), true);
        return responses.responses().values().stream().allMatch(PaxosResponse::isSuccessful);
    }
}
