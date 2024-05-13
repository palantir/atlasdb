/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.migration.actions;

import com.google.common.math.DoubleMath;
import com.palantir.atlasdb.workload.migration.jmx.CassandraStateManager;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;

public class SetInterDcStreamThroughput implements MigrationAction {
    private static final SafeLogger log = SafeLoggerFactory.get(SetInterDcStreamThroughput.class);
    private static final double STREAM_THROUGHPUT_IN_MBPS = 0.0001;

    private final CassandraStateManager stateManager;

    public SetInterDcStreamThroughput(CassandraStateManager stateManager) {
        this.stateManager = stateManager;
    }

    @Override
    public void runForwardStep() {
        stateManager.setInterDcStreamThroughput(STREAM_THROUGHPUT_IN_MBPS);
        log.info(
                "Set inter-DC stream throughput to {} Mb/s", SafeArg.of("throughputInMbps", STREAM_THROUGHPUT_IN_MBPS));
    }

    @Override
    public boolean isApplied() {
        double currentInterDcStreamThroughput = stateManager.getInterDcStreamThroughput();
        log.info(
                "Current inter-dc stream throughput is {} Mb/s",
                SafeArg.of("throughputInMbps", currentInterDcStreamThroughput));
        return DoubleMath.fuzzyEquals(currentInterDcStreamThroughput, STREAM_THROUGHPUT_IN_MBPS, 0.0001);
    }
}
