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

package com.palantir.timelock.config;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

public final class TimeLockPersistenceInvariants {
    private TimeLockPersistenceInvariants() {
        // No
    }

    public static void checkPersistenceConsistentWithState(boolean isNewService, boolean directoriesExist) {
        if (isNewService && directoriesExist) {
            throw new SafeIllegalArgumentException(
                    "This timelock server has been configured as a new stack but the Paxos data directories already "
                            + "exist. Almost surely this is because it has already been turned on at least once, and "
                            + "thus the 'is-new-service' property should be set to false for safety reasons, or if "
                            + "you are performing a timelock migration you should remove this node from the known "
                            + "bootstrapping node list.");
        }
        if (!isNewService && !directoriesExist) {
            throw new SafeIllegalArgumentException("The timelock data directories do not appear to exist. If you are "
                    + "trying to move the nodes on your timelock cluster or add new nodes, you have likely already "
                    + "made a mistake by this point. This is a non-trivial operation and risks service corruption, "
                    + "so contact support for assistance. Otherwise, if this is a new timelock service, please "
                    + "configure paxos.is-new-service to true for the first startup only of each node. Alternatively, "
                    + "if you are migrating timelock, please add this node to the known bootstrapping list.");
        }
    }
}
