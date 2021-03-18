/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.LeaderTimes;
import com.palantir.atlasdb.timelock.api.MultiClientConjureTimelockServiceBlocking;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Map;
import java.util.Set;

public class AuthenticatedInternalMultiClientConjureTimelockService
        implements InternalMultiClientConjureTimelockService {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");

    private final MultiClientConjureTimelockServiceBlocking delegate;

    public AuthenticatedInternalMultiClientConjureTimelockService(MultiClientConjureTimelockServiceBlocking delegate) {
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
        return delegate.startTransactionsForClients(AUTH_HEADER, requests);
    }
}
