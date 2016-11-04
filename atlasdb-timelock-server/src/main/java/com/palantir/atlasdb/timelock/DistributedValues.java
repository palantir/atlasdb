/**
 * Copyright 2016 Palantir Technologies
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

import com.google.common.util.concurrent.Futures;

import io.atomix.AtomixReplica;
import io.atomix.group.DistributedGroup;
import io.atomix.variables.DistributedLong;
import io.atomix.variables.DistributedValue;

public final class DistributedValues {
    private DistributedValues() {
        // Utility class
    }

    public static DistributedValue<String> getLeaderId(AtomixReplica replica) {
        return Futures.getUnchecked(replica.<String>getValue("atlasdb/leader"));
    }

    public static DistributedLong getTimestampForClient(AtomixReplica replica, String client) {
        return Futures.getUnchecked(replica.getLong("atlasdb/timestamp/" + client));
    }

    public static DistributedGroup getTimeLockGroup(AtomixReplica replica) {
        return Futures.getUnchecked(replica.getGroup("atlasdb/timelock"));
    }
}
