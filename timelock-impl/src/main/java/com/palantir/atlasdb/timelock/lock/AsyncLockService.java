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

import java.math.BigInteger;
import java.util.Set;

import com.palantir.lock.HeldLocksGrant;
import com.palantir.lock.HeldLocksToken;
import com.palantir.lock.LockClient;
import com.palantir.lock.LockRequest;
import com.palantir.lock.LockResponse;
import com.palantir.lock.LockServerOptions;
import com.palantir.lock.SimpleHeldLocksToken;
import com.palantir.processors.AutoDelegate;

@AutoDelegate
public interface AsyncLockService extends AsyncRemoteLockService {
    LockResponse lockWithFullLockResponse(LockClient client, LockRequest request) throws InterruptedException;
    boolean unlock(HeldLocksToken token);
    boolean unlockSimple(SimpleHeldLocksToken token);
    boolean unlockAndFreeze(HeldLocksToken token);
    Set<HeldLocksToken> getTokens(LockClient client);
    Set<HeldLocksToken> refreshTokens(Iterable<HeldLocksToken> tokens);
    HeldLocksGrant refreshGrant(HeldLocksGrant grant);
    HeldLocksGrant refreshGrant(BigInteger grantId);
    HeldLocksGrant convertToGrant(HeldLocksToken token);
    HeldLocksToken useGrant(LockClient client, HeldLocksGrant grant);
    HeldLocksToken useGrant(LockClient client, BigInteger grantId);
    Long getMinLockedInVersionId();
    Long getMinLockedInVersionId(LockClient client);
    LockServerOptions getLockServerOptions();
    long currentTimeMillis();
    void logCurrentState();
}
