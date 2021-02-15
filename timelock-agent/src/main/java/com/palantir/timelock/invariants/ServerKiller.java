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

import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ServerKiller {
    private static final Logger log = LoggerFactory.getLogger(ServerKiller.class);

    private ServerKiller() {
        // no
    }

    public static Error killMeNow(Throwable error) {
        log.error(
                "Something bad happened and we can't continue safely, so we're preemptively killing the server.",
                error);
        System.exit(1);
        throw new SafeIllegalStateException("We should have exited before we get to this point", error);
    }
}
