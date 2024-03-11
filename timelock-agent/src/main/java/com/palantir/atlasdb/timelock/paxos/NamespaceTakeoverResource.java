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

package com.palantir.atlasdb.timelock.paxos;

import com.palantir.atlasdb.timelock.paxos.api.NamespaceLeadershipTakeoverService;
import com.palantir.tokens.auth.AuthHeader;
import java.util.Set;

final class NamespaceTakeoverResource implements NamespaceLeadershipTakeoverService {
    private final NamespaceTakeoverComponent delegate;

    NamespaceTakeoverResource(NamespaceTakeoverComponent delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean takeover(AuthHeader _authHeader, String namespace) {
        return delegate.takeover(namespace);
    }

    @Override
    public Set<String> takeoverNamespaces(AuthHeader _authHeader, Set<String> namespaces) {
        return delegate.takeoverNamespaces(namespaces);
    }
}
