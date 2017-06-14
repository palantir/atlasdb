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

package com.palantir.atlasdb.timelock.perf;

import java.util.Map;

import com.palantir.atlasdb.config.AtlasDbConfig;

public class PerfTestResource implements PerfTestService {

    private final AtlasDbConfig config;

    public PerfTestResource(AtlasDbConfig config) {
        this.config = config;
    }

    @Override
    public Map<String, Object> writeTransactionPerf(int numClients, int numRequestsPerClient) {
        return WriteTransactionPerfTest.execute(config, numClients, numRequestsPerClient);
    }

    @Override
    public Map<String, Object> readTransactionPerf(int numClients, int numRequestsPerClient) {
        return ReadTransactionPerfTest.execute(config, numClients, numRequestsPerClient);
    }

    @Override
    public Map<String, Object> kvsWritePerf(int numClients, int numRequestsPerClient) {
        return KvsWritePerfTest.execute(config, numClients, numRequestsPerClient);
    }

    @Override
    public Map<String, Object> kvsCasPerf(int numClients, int numRequestsPerClient) {
        return KvsCasPerfTest.execute(config, numClients, numRequestsPerClient);
    }

    @Override
    public Map<String, Object> kvsReadPerf(int numClients, int numRequestsPerClient) {
        return null;
    }

    @Override
    public Map<String, Object> timestampPerf(int numClients, int numRequestsPerClient) {
        return TimestampPerfTest.execute(config, numClients, numRequestsPerClient);
    }
}
