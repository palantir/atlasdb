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

package com.palantir.atlasdb.timelock.management;

import com.palantir.atlasdb.timelock.TimeLockServices;
import com.palantir.atlasdb.timelock.TimelockNamespaces;
import com.palantir.timestamp.TimestampService;

public class NamespacedConsensus {
    /**
     * Gets a fresh timestamp for namespace from {@link TimestampService#getFreshTimestamp()} and fast forwards
     * the timestamp by 1 million for the given namespace.
     *
     * @param namespace namespace for which consensus has to be achieved
     */
    public static void achieveConsensusForNamespace(TimelockNamespaces timelockNamespaces, String namespace) {
        TimeLockServices timeLockServices = timelockNamespaces.get(namespace);
        long timestamp = timeLockServices.getTimelockService().getFreshTimestamp() + 1000000L;
        timeLockServices.getTimestampManagementService().fastForwardTimestamp(timestamp);
    }
}
