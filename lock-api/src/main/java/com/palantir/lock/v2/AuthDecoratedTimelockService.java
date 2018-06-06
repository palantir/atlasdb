/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.lock.v2;

import java.util.Set;
import java.util.function.Supplier;

import com.palantir.timestamp.TimestampRange;
import com.palantir.tokens.auth.AuthHeader;

public class AuthDecoratedTimelockService implements TimelockService {
    private final AuthedTimelockService authedTimelockService;
    private final Supplier<AuthHeader> authHeaderSupplier;

    public AuthDecoratedTimelockService(AuthedTimelockService authedTimelockService,
            Supplier<AuthHeader> authHeaderSupplier) {
        this.authedTimelockService = authedTimelockService;
        this.authHeaderSupplier = authHeaderSupplier;
    }

    @Override
    public long getFreshTimestamp() {
        return authedTimelockService.getFreshTimestamp(authHeaderSupplier.get());
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return authedTimelockService.getFreshTimestamps(authHeaderSupplier.get(), numTimestampsRequested);
    }

    @Override
    public LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request) {
        return authedTimelockService.lockImmutableTimestamp(authHeaderSupplier.get(), request);
    }

    @Override
    public long getImmutableTimestamp() {
        return authedTimelockService.getImmutableTimestamp(authHeaderSupplier.get());
    }

    @Override
    public LockResponse lock(LockRequest request) {
        return authedTimelockService.lock(authHeaderSupplier.get(), request);
    }

    @Override
    public WaitForLocksResponse waitForLocks(WaitForLocksRequest request) {
        return authedTimelockService.waitForLocks(authHeaderSupplier.get(), request);
    }

    @Override
    public Set<LockToken> refreshLockLeases(Set<LockToken> tokens) {
        return authedTimelockService.refreshLockLeases(authHeaderSupplier.get(), tokens);
    }

    @Override
    public Set<LockToken> unlock(Set<LockToken> tokens) {
        return authedTimelockService.unlock(authHeaderSupplier.get(), tokens);
    }

    @Override
    public long currentTimeMillis() {
        return authedTimelockService.currentTimeMillis(authHeaderSupplier.get());
    }
}
