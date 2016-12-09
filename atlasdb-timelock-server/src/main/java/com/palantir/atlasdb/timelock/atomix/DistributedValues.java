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
package com.palantir.atlasdb.timelock.atomix;

import io.atomix.Atomix;
import io.atomix.group.DistributedGroup;
import io.atomix.variables.DistributedLong;
import io.atomix.variables.DistributedValue;

public final class DistributedValues {
    private DistributedValues() {
        // Utility class
    }

    public static DistributedValue<LeaderAndTerm> getLeaderInfo(Atomix atomix) {
        return AtomixRetryer.getWithRetry(() -> atomix.<LeaderAndTerm>getValue("atlasdb/leader"));
    }

    public static DistributedLong getTimestampForClient(Atomix atomix, String client) {
        return AtomixRetryer.getWithRetry(() -> atomix.getLong("atlasdb/timestamp/" + client));
    }

    public static DistributedGroup getTimeLockGroup(Atomix atomix) {
        return AtomixRetryer.getWithRetry(() -> atomix.getGroup("atlasdb/timestamp"));
    }
}
