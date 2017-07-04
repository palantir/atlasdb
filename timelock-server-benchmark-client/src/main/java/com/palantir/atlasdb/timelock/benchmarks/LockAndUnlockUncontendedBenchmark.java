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

package com.palantir.atlasdb.timelock.benchmarks;

import java.util.Map;

import com.palantir.atlasdb.transaction.impl.SerializableTransactionManager;
import com.palantir.lock.RemoteLockService;

public class LockAndUnlockUncontendedBenchmark extends LockAndUnlockContendedBenchmark {

    public static Map<String, Object> execute(SerializableTransactionManager txnManager, int numClients,
            int requestsPerClient) {
        return new LockAndUnlockUncontendedBenchmark(txnManager.getLockService(), numClients, requestsPerClient).execute();
    }

    private LockAndUnlockUncontendedBenchmark(RemoteLockService lockService, int numClients, int numRequestsPerClient) {
        super(lockService, numClients, numRequestsPerClient, numClients * numRequestsPerClient);
    }

}
