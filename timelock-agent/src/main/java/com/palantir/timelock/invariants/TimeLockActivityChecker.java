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

package com.palantir.timelock.invariants;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.lock.v2.TimelockRpcClient;
import com.palantir.logsafe.SafeArg;

public class TimeLockActivityChecker {
    private static final Logger log = LoggerFactory.getLogger(TimeLockActivityChecker.class);

    private final TimelockRpcClient timelockRpcClient;

    public TimeLockActivityChecker(TimelockRpcClient timelockRpcClient) {
        this.timelockRpcClient = timelockRpcClient;
    }

    public boolean isThisNodeActivelyServingTimestampsForClient(String client) {
        try {
            timelockRpcClient.getFreshTimestamp(client);
            return true;
        } catch (Exception e) {
            log.debug("Suppressed exception when checking TimeLock activity for client {}",
                    SafeArg.of("client", client),
                    e);
            return false;
        }
    }
}
