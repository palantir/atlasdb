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

package com.palantir.atlasdb.timelock.benchmarks.benchmarks;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.transaction.api.TransactionManager;
import com.palantir.timestamp.TimestampService;

public final class TimestampBenchmark extends AbstractBenchmark {

    private final TimestampService timestampService;

    public static Map<String, Object> execute(TransactionManager txnManager, int numClients,
            int requestsPerClient) {
        return new TimestampBenchmark(txnManager.getTimestampService(), numClients, requestsPerClient).execute();
    }

    private TimestampBenchmark(TimestampService timestampService, int numClients, int numRequestsPerClient) {
        super(numClients, numRequestsPerClient);
        this.timestampService = timestampService;
    }

    @Override
    protected void performOneCall() {
        long timestamp = timestampService.getFreshTimestamp();
        Preconditions.checkState(timestamp > 0);
    }
}
