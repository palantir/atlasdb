/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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

import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.TimelockService;
import com.palantir.logsafe.SafeArg;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Releases lock tokens from a {@link TimelockService} asynchronously.
 *
 * There is another layer of retrying below us (at the HTTP client level) for external timelock users.
 * Also, in the event we fail to unlock (e.g. because of a connection issue), locks will eventually time-out.
 * Thus not retrying is reasonably safe (as long as we can guarantee that the lock won't otherwise be refreshed).
 */
public final class AsyncTimeLockUnlocker implements TimeLockUnlocker, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(AsyncTimeLockUnlocker.class);
    private final DisruptorAutobatcher<Set<LockToken>, Void> autobatcher;

    private AsyncTimeLockUnlocker(DisruptorAutobatcher<Set<LockToken>, Void> autobatcher) {
        this.autobatcher = autobatcher;
    }

    public static AsyncTimeLockUnlocker create(TimelockService timelockService) {
        return new AsyncTimeLockUnlocker(
                Autobatchers.<Set<LockToken>, Void>independent(batch -> {
                    Set<LockToken> allTokensToUnlock = batch.stream()
                            .map(BatchElement::argument)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toSet());
                    try {
                        timelockService.tryUnlock(allTokensToUnlock);
                    } catch (Throwable t) {
                        log.info("Failed to unlock lock tokens {} from timelock. They will eventually expire on their "
                                        + "own, but if this message recurs frequently, it may be worth investigation.",
                                SafeArg.of("numFailed", allTokensToUnlock.size()),
                                t);
                    }
                    batch.stream().map(BatchElement::result).forEach(f -> f.set(null));
                })
                        .safeLoggablePurpose("async-timelock-unlocker")
                        .build());
    }

    /**
     * Adds all provided lock tokens to a queue to eventually be scheduled for unlocking.
     * Locks in the queue are unlocked asynchronously, and users must not depend on these locks being unlocked /
     * available for other users immediately.
     *
     * This may block, if continuing to buffer may cause memory pressure issues.
     *
     * @param tokens Lock tokens to schedule an unlock for.
     */
    @Override
    @SuppressWarnings("FutureReturnValueIgnored")
    public void enqueue(Set<LockToken> tokens) {
        autobatcher.apply(tokens);
    }

    @Override
    public void close() {
        autobatcher.close();
    }
}
