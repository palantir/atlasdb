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

package com.palantir.lock.impl;

import java.math.BigInteger;
import java.util.Set;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleHeldLocksToken;

//@AutoDelegate
public interface AsyncLockService extends AsyncRemoteLockService {
    ListenableFuture<LockResponse> lockWithFullLockResponse(LockClient client, LockRequest request);
    ListenableFuture<Boolean> unlock(HeldLocksToken token);
    ListenableFuture<Boolean> unlockSimple(SimpleHeldLocksToken token);
    ListenableFuture<Boolean> unlockAndFreeze(HeldLocksToken token);
    ListenableFuture<Set<HeldLocksToken>> getTokens(LockClient client);
    ListenableFuture<Set<HeldLocksToken>> refreshTokens(Iterable<HeldLocksToken> tokens);
    ListenableFuture<HeldLocksGrant> refreshGrant(HeldLocksGrant grant);
    ListenableFuture<HeldLocksGrant> refreshGrant(BigInteger grantId);
    ListenableFuture<HeldLocksGrant> convertToGrant(HeldLocksToken token);
    ListenableFuture<HeldLocksToken> useGrant(LockClient client, HeldLocksGrant grant);
    ListenableFuture<HeldLocksToken> useGrant(LockClient client, BigInteger grantId);
    ListenableFuture<Long> getMinLockedInVersionId();
    ListenableFuture<Long> getMinLockedInVersionId(LockClient client);
    ListenableFuture<LockServerOptions> getLockServerOptions();
    ListenableFuture<Long> currentTimeMillis();
    ListenableFuture<Void> logCurrentState();
}
