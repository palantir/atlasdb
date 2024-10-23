/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.paxos;

import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.GetMinLeasedTimestampRequests;
import com.palantir.atlasdb.timelock.api.GetMinLeasedTimestampResponses;
import com.palantir.atlasdb.timelock.api.LeaderTimes;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockService;
import com.palantir.atlasdb.timelock.api.MultiClientGetMinLeasedTimestampRequest;
import com.palantir.atlasdb.timelock.api.MultiClientTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.NamespaceTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.NamespaceTimestampLeaseResponse;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Map;
import java.util.Set;

final class InMemoryInternalMultiClientConjureTimelockService implements InternalMultiClientConjureTimelockService {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");

    private final MultiClientConjureTimelockService delegate;

    InMemoryInternalMultiClientConjureTimelockService(MultiClientConjureTimelockService delegate) {
        this.delegate = delegate;
    }

    @Override
    public LeaderTimes leaderTimes(Set<Namespace> namespaces) {
        return delegate.leaderTimes(AUTH_HEADER, namespaces);
    }

    @Override
    public Map<Namespace, GetCommitTimestampsResponse> getCommitTimestamps(
            Map<Namespace, GetCommitTimestampsRequest> requests) {
        return delegate.getCommitTimestampsForClients(AUTH_HEADER, requests);
    }

    @Override
    public Map<Namespace, ConjureStartTransactionsResponse> startTransactions(
            Map<Namespace, ConjureStartTransactionsRequest> requests) {
        return delegate.startTransactions(AUTH_HEADER, requests);
    }

    @Override
    public Map<Namespace, ConjureUnlockResponseV2> unlock(Map<Namespace, ConjureUnlockRequestV2> requests) {
        return delegate.unlock(AUTH_HEADER, requests);
    }

    @Override
    public Map<Namespace, NamespaceTimestampLeaseResponse> acquireTimestampLeases(
            Map<Namespace, NamespaceTimestampLeaseRequest> requests) {
        return delegate.acquireTimestampLease(AUTH_HEADER, MultiClientTimestampLeaseRequest.of(requests))
                .get();
    }

    @Override
    public Map<Namespace, GetMinLeasedTimestampResponses> getMinLeasedTimestamps(
            Map<Namespace, GetMinLeasedTimestampRequests> requests) {
        return delegate.getMinLeasedTimestamp(AUTH_HEADER, MultiClientGetMinLeasedTimestampRequest.of(requests))
                .get();
    }
}
