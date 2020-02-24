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
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.atlasdb.timelock.api.StartTransactionsWithWatchesRequest;
import com.palantir.atlasdb.timelock.api.StartTransactionsWithWatchesResponse;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.tokens.auth.AuthHeader;

public class NamespacedConjureTimelockService {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");
    private final String namespace;
    private final ConjureTimelockService conjureTimelockService;

    public NamespacedConjureTimelockService(ConjureTimelockService conjureTimelockService, String namespace) {
        this.namespace = namespace;
        this.conjureTimelockService = conjureTimelockService;
    }

    public ConjureStartTransactionsResponse startTransactions(ConjureStartTransactionsRequest request) {
        return conjureTimelockService.startTransactions(AUTH_HEADER, namespace, request);
    }

    public StartTransactionsWithWatchesResponse startTransactionsWithWatches(StartTransactionsWithWatchesRequest req) {
        return conjureTimelockService.startTransactionsWithWatches(AUTH_HEADER, namespace, req);
    }

    public LeaderTime leaderTime() {
        return conjureTimelockService.leaderTime(AUTH_HEADER, namespace);
    }
}
