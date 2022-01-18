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
import com.palantir.atlasdb.timelock.api.DisabledNamespacesRequest;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesResponse;
import com.palantir.atlasdb.timelock.api.DisabledNamespacesUpdaterService;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import com.palantir.paxos.BooleanPaxosResponse;
import com.palantir.paxos.PaxosQuorumChecker;
import com.palantir.paxos.PaxosResponse;
import com.palantir.paxos.PaxosResponsesWithRemote;
import com.palantir.tokens.auth.AuthHeader;
import java.time.Duration;
import java.util.Map;
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

    // TODO(gs): retry logic
    public DisabledNamespacesResponse updateAllNodes(DisabledNamespacesRequest update) {
        boolean pingSuccess = attemptOnAllNodes(service -> new BooleanPaxosResponse(service.ping(authHeader)));

        if (!pingSuccess) {
            // TODO(gs): exception
            return DisabledNamespacesResponse.of(false);
        }

        Function<DisabledNamespacesUpdaterService, PaxosResponse> request =
                service -> wrapModification(service, update);
        boolean updateSuccess = attemptOnAllNodes(request);

        if (updateSuccess) {
            localUpdater.applyModification(update);
            // TODO(gs): fall back if fails
            return DisabledNamespacesResponse.of(true);
        }

        // if all else fails, do the opposite thing
        // TODO(gs): if _this_ fails, log something sensible so the user can fix themselves
        //      (safe to assume this is already monitored)
        DisabledNamespacesRequest rollbackRequest = DisabledNamespacesRequest.builder()
                .from(update)
                .setEnabled(!update.getSetEnabled())
                .build();
        boolean rollbackSuccess = attemptOnAllNodes(service -> wrapModification(service, rollbackRequest));
        if (rollbackSuccess) {
            return DisabledNamespacesResponse.of(false);
        }

        throw new SafeRuntimeException("ohno");
    }

    private BooleanPaxosResponse wrapModification(
            DisabledNamespacesUpdaterService service, DisabledNamespacesRequest update) {
        return new BooleanPaxosResponse(
                service.applyModification(authHeader, update).getWasSuccessful());
    }

    private boolean attemptOnAllNodes(Function<DisabledNamespacesUpdaterService, PaxosResponse> request) {
        PaxosResponsesWithRemote<DisabledNamespacesUpdaterService, PaxosResponse> responses =
                PaxosQuorumChecker.collectQuorumResponses(
                        updaters, request, updaters.size(), executors, Duration.ofSeconds(5L), true);
        return responses.responses().values().stream().allMatch(PaxosResponse::isSuccessful);
    }
}
