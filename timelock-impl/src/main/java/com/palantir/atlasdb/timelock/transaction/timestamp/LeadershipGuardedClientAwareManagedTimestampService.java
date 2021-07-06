/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.transaction.timestamp;

import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.timestamp.TimestampRange;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ClientAwareManagedTimestampService} that also verifies that it is not known to not be the leader before
 * returning results to the user.
 */
public class LeadershipGuardedClientAwareManagedTimestampService
        implements ClientAwareManagedTimestampService, AutoCloseable {
    private static final Logger log =
            LoggerFactory.getLogger(LeadershipGuardedClientAwareManagedTimestampService.class);

    private final ClientAwareManagedTimestampService delegate;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public LeadershipGuardedClientAwareManagedTimestampService(ClientAwareManagedTimestampService delegate) {
        this.delegate = delegate;
    }

    @Override
    public PartitionedTimestamps getFreshTimestampsForClient(UUID clientIdentifier, int numTimestampsRequested) {
        return ensureStillOpen(() -> delegate.getFreshTimestampsForClient(clientIdentifier, numTimestampsRequested));
    }

    @Override
    public void fastForwardTimestamp(long currentTimestamp) {
        ensureStillOpen(() -> {
            delegate.fastForwardTimestamp(currentTimestamp);
            return null;
        });
    }

    @Override
    public String ping() {
        return ensureStillOpen(delegate::ping);
    }

    @Override
    public boolean isInitialized() {
        return ensureStillOpen(delegate::isInitialized);
    }

    @Override
    public long getFreshTimestamp() {
        return ensureStillOpen(delegate::getFreshTimestamp);
    }

    @Override
    public TimestampRange getFreshTimestamps(int numTimestampsRequested) {
        return ensureStillOpen(() -> delegate.getFreshTimestamps(numTimestampsRequested));
    }

    @Override
    public void close() {
        if (!isClosed.compareAndSet(false, true)) {
            log.info("Could not close the client-aware managed timestamp service, because it was already closed."
                    + " Possibly indicative of weirdness in Atlas code, but should be benign as far as the user is"
                    + " concerned.");
        }
    }

    private <T> T ensureStillOpen(Supplier<T> operation) {
        T value = operation.get();
        // Ensure that no one else may have made a new timestamp service and invalidated us
        throwIfClosed();
        return value;
    }

    private void throwIfClosed() {
        if (isClosed.get()) {
            throw new NotCurrentLeaderException("Lost leadership elsewhere");
        }
    }
}
