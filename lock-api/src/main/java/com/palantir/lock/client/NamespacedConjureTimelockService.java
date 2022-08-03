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

import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureSingleTimestamp;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.lock.v2.LeaderTime;

public interface NamespacedConjureTimelockService {
    ConjureUnlockResponse unlock(ConjureUnlockRequest request);

    ConjureUnlockResponseV2 unlockV2(ConjureUnlockRequestV2 request);

    ConjureRefreshLocksResponse refreshLocks(ConjureRefreshLocksRequest request);

    ConjureRefreshLocksResponseV2 refreshLocksV2(ConjureRefreshLocksRequestV2 request);

    ConjureWaitForLocksResponse waitForLocks(ConjureLockRequest request);

    ConjureLockResponse lock(ConjureLockRequest request);

    LeaderTime leaderTime();

    GetCommitTimestampsResponse getCommitTimestamps(GetCommitTimestampsRequest request);

    ConjureGetFreshTimestampsResponseV2 getFreshTimestampsV2(ConjureGetFreshTimestampsRequestV2 request);

    ConjureSingleTimestamp getFreshTimestamp();

    ConjureStartTransactionsResponse startTransactions(ConjureStartTransactionsRequest request);
}
