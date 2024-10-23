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

package com.palantir.lock.client.timestampleases;

import com.palantir.atlasdb.timelock.api.GetMinLeasedTimestampRequests;
import com.palantir.atlasdb.timelock.api.GetMinLeasedTimestampResponses;
import com.palantir.atlasdb.timelock.api.Namespace;
import com.palantir.atlasdb.timelock.api.NamespaceTimestampLeaseRequest;
import com.palantir.atlasdb.timelock.api.NamespaceTimestampLeaseResponse;
import com.palantir.lock.client.InternalMultiClientConjureTimelockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.util.Map;

public final class NamespacedTimestampLeaseServiceImpl implements NamespacedTimestampLeaseService {
    private final Namespace namespace;
    private final InternalMultiClientConjureTimelockService delegate;

    public NamespacedTimestampLeaseServiceImpl(
            Namespace namespace, InternalMultiClientConjureTimelockService delegate) {
        this.namespace = namespace;
        this.delegate = delegate;
    }

    @Override
    public NamespaceTimestampLeaseResponse acquireTimestampLeases(NamespaceTimestampLeaseRequest request) {
        Map<Namespace, NamespaceTimestampLeaseResponse> response =
                delegate.acquireTimestampLeases(Map.of(namespace, request));
        validateResponseIsCoherent(response);
        return response.get(namespace);
    }

    @Override
    public GetMinLeasedTimestampResponses getMinLeasedTimestamps(GetMinLeasedTimestampRequests request) {
        Map<Namespace, GetMinLeasedTimestampResponses> response =
                delegate.getMinLeasedTimestamps(Map.of(namespace, request));
        validateResponseIsCoherent(response);
        return response.get(namespace);
    }

    private void validateResponseIsCoherent(Map<Namespace, ?> response) {
        if (response.size() != 1) {
            throw new SafeRuntimeException(
                    "Response should include exactly one namespace result",
                    SafeArg.of("namespace", namespace),
                    SafeArg.of("response", response));
        }

        if (!response.containsKey(namespace)) {
            throw new SafeRuntimeException(
                    "Response should include the entry for the relevant namespace",
                    SafeArg.of("namespace", namespace),
                    SafeArg.of("response", response));
        }
    }
}
