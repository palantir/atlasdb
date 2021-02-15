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

package com.palantir.lock.client;

import com.google.common.collect.Iterables;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.logsafe.SafeArg;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class LockWatchLogUtility {
    private static final Logger log = LoggerFactory.getLogger(LockWatchLogUtility.class);

    private LockWatchLogUtility() {
        // no op
    }

    static void logTransactionEvents(Optional<LockWatchVersion> requestedVersion, LockWatchStateUpdate update) {
        if (log.isDebugEnabled()) {
            Optional<LockWatchStateUpdate.Success> successfulUpdate =
                    update.accept(new LockWatchStateUpdate.Visitor<Optional<LockWatchStateUpdate.Success>>() {
                        @Override
                        public Optional<LockWatchStateUpdate.Success> visit(LockWatchStateUpdate.Success success) {
                            return Optional.of(success);
                        }

                        @Override
                        public Optional<LockWatchStateUpdate.Success> visit(LockWatchStateUpdate.Snapshot snapshot) {
                            return Optional.empty();
                        }
                    });

            successfulUpdate.ifPresent(success -> log.debug(
                    "Lock watch state update information",
                    SafeArg.of("requestedVersion", requestedVersion),
                    SafeArg.of("responseFirstVersion", Iterables.getFirst(success.events(), null)),
                    SafeArg.of("responseLatestVersion", success.lastKnownVersion())));
        }
    }
}
