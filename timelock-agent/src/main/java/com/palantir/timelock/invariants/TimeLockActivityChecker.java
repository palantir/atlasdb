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

import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureTimelockService;
import com.palantir.logsafe.SafeArg;
import com.palantir.tokens.auth.AuthHeader;
import java.util.OptionalLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeLockActivityChecker {
    private static final Logger log = LoggerFactory.getLogger(TimeLockActivityChecker.class);
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer unused");

    private final ConjureTimelockService conjureTimelockService;

    public TimeLockActivityChecker(ConjureTimelockService conjureTimelockService) {
        this.conjureTimelockService = conjureTimelockService;
    }

    public OptionalLong getFreshTimestampFromNodeForClient(String client) {
        try {
            return OptionalLong.of(conjureTimelockService
                    .getFreshTimestamps(AUTH_HEADER, client, ConjureGetFreshTimestampsRequest.of(1))
                    .getInclusiveLower());
        } catch (Exception e) {
            log.info(
                    "Suppressed exception when checking TimeLock activity for client {}",
                    SafeArg.of("client", client),
                    e);
            return OptionalLong.empty();
        }
    }
}
