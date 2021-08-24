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
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.timestamp.TimestampRange;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * A {@link ClientAwareManagedTimestampService} that also verifies that it is not known to not be the leader before
 * returning results to the user.
 */
public class LeadershipGuardedClientAwareManagedTimestampService
        implements ClientAwareManagedTimestampService, AutoCloseable {
    private static final SafeLogger log =
            SafeLoggerFactory.get(LeadershipGuardedClientAwareManagedTimestampService.class);

    private final ClientAwareManagedTimestampService delegate;
    private volatile boolean isClosed = false;

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
            // This looks funky, but is intentional:
            // although it doesn't prevent the fast forward from happening as far as the Paxos log is concerned,
            // a user will never know that their fast forward succeeded. The log is able to subsequently recover
            // (it will find that the UUID for the proposer is not the one it expects, and cause another leader
            // election, but that's fine to help us get out of this race condition.)
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
        isClosed = true;
    }

    private <T> T ensureStillOpen(Supplier<T> operation) {
        T value = operation.get();
        // Ensure that no one else may have made a new timestamp service and invalidated us
        throwIfClosed();
        return value;
    }

    private void throwIfClosed() {
        if (isClosed) {
            throw new NotCurrentLeaderException("Lost leadership elsewhere");
        }
    }
}
