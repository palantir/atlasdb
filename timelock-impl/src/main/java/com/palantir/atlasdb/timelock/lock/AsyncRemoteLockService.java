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

package com.palantir.atlasdb.timelock.lock;

import java.util.Set;

import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockRefreshToken;
import com.palantir.lock.LockRequest;

public interface AsyncRemoteLockService {
    ListenableFuture<LockRefreshToken> lock(String client, LockRequest request);
    ListenableFuture<HeldLocksToken> lockAndGetHeldLocks(String client, LockRequest request);
    ListenableFuture<Boolean> unlock(LockRefreshToken token);
    ListenableFuture<Set<LockRefreshToken>> refreshLockRefreshTokens(Iterable<LockRefreshToken> tokens);
    ListenableFuture<Long> getMinLockedInVersionId(String client);
    ListenableFuture<Long> currentTimeMillis();
    ListenableFuture<Void> logCurrentState();
}
