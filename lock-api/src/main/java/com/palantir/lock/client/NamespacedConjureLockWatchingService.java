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

import com.palantir.atlasdb.timelock.api.LockWatchRequest;
import com.palantir.atlasdb.timelock.lock.watch.ConjureLockWatchingServiceBlocking;
import com.palantir.lock.watch.LockWatchStarter;
import com.palantir.tokens.auth.AuthHeader;

public class NamespacedConjureLockWatchingService implements LockWatchStarter {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");

    private final ConjureLockWatchingServiceBlocking lockWatcher;
    private final String namespace;

    public NamespacedConjureLockWatchingService(ConjureLockWatchingServiceBlocking lockWatcher, String namespace) {
        this.lockWatcher = lockWatcher;
        this.namespace = namespace;
    }

    public void startWatching(LockWatchRequest request) {
        lockWatcher.startWatching(AUTH_HEADER, namespace, request);
    }
}
