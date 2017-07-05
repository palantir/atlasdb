/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import java.io.Closeable;
import java.util.Set;

import com.palantir.atlasdb.timelock.lock.AsyncResult;
import com.palantir.atlasdb.timelock.paxos.ManagedTimestampService;
import com.palantir.lock.v2.LockImmutableTimestampRequest;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockRequestV2;
import com.palantir.lock.v2.LockTokenV2;
import com.palantir.lock.v2.WaitForLocksRequest;

public interface AsyncTimelockService extends ManagedTimestampService, Closeable {

    long currentTimeMillis();

    Set<LockTokenV2> unlock(Set<LockTokenV2> tokens);

    Set<LockTokenV2> refreshLockLeases(Set<LockTokenV2> tokens);

    AsyncResult<Void> waitForLocks(WaitForLocksRequest request);

    AsyncResult<LockTokenV2> lock(LockRequestV2 request);

    long getImmutableTimestamp();

    LockImmutableTimestampResponse lockImmutableTimestamp(LockImmutableTimestampRequest request);

}
