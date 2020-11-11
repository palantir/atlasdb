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

package com.palantir.atlasdb.keyvalue.api.watch;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.keyvalue.api.watch.TimestampStateStore.CommitInfo;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.client.LeasedLockToken;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.ImmutableInvalidateAll;
import com.palantir.lock.watch.ImmutableInvalidateSome;
import com.palantir.lock.watch.ImmutableTransactionsLockWatchUpdate;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.lock.watch.UnlockEvent;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.immutables.value.Value;

@Value.Immutable
interface ClientLogEvents {

    LockWatchEvents events();

    boolean clearCache();

    @Value.Derived
    default Optional<Long> latestVersion() {
        return events().versionRange().map(Range::upperEndpoint);
    }

    default TransactionsLockWatchUpdate toTransactionsLockWatchUpdate(
            TimestampMapping timestampMapping, Optional<LockWatchVersion> lastKnownVersion) {
        /*
         Case 1: client is behind earliest transaction. Therefore we want to ensure that there are events present for
                 each version starting with the client version (exclusive) and ending with latest transaction version
                 (inclusive).
         Case 2: client is at least as up-to-date as the earliest transaction. The check here is the same as above.
         Case 3: client is completely up-to-date. Here, we don't need to check for any versions.
         Case 4: client has no version. Then we expect that the events returned at least enclose the versions of
                 the transactions.
        */
        verifyReturnedEventsEnclosesTransactionVersions(
                lastKnownVersion
                        .map(LockWatchVersion::version)
                        .map(version -> version + 1)
                        .orElseGet(() -> timestampMapping.versionRange().lowerEndpoint()),
                timestampMapping.versionRange().upperEndpoint());
        return ImmutableTransactionsLockWatchUpdate.builder()
                .startTsToSequence(timestampMapping.timestampMapping())
                .events(events().events())
                .clearCache(clearCache())
                .build();
    }

    default CommitUpdate toCommitUpdate(LockWatchVersion startVersion, CommitInfo commitInfo) {
        if (clearCache()) {
            return ImmutableInvalidateAll.builder().build();
        }

        // We want to ensure that we do not miss any versions, but we do not care about the event with the same version
        // as the start version.
        verifyReturnedEventsEnclosesTransactionVersions(
                startVersion.version() + 1, commitInfo.commitVersion().version());

        LockEventVisitor eventVisitor = new LockEventVisitor(commitInfo.commitLockToken());
        Set<LockDescriptor> locksTakenOut = new HashSet<>();
        events().events().forEach(event -> locksTakenOut.addAll(event.accept(eventVisitor)));
        return ImmutableInvalidateSome.builder().invalidatedLocks(locksTakenOut).build();
    }

    default void verifyReturnedEventsEnclosesTransactionVersions(long lowerBound, long upperBound) {
        if (lowerBound > upperBound) {
            return;
        }

        Range<Long> rangeToTest = Range.closed(lowerBound, upperBound);
        events().versionRange().ifPresent(eventsRange -> {
            if (!eventsRange.encloses(rangeToTest)) {
                throw new TransactionLockWatchFailedException("Events do not enclose the required versions");
            }
        });
    }

    class Builder extends ImmutableClientLogEvents.Builder {}

    final class LockEventVisitor implements LockWatchEvent.Visitor<Set<LockDescriptor>> {
        private final Optional<UUID> commitRequestId;

        private LockEventVisitor(LockToken commitLocksToken) {
            if (commitLocksToken instanceof LeasedLockToken) {
                commitRequestId = Optional.of(
                        ((LeasedLockToken) commitLocksToken).serverToken().getRequestId());
            } else {
                commitRequestId = Optional.empty();
            }
        }

        @Override
        public Set<LockDescriptor> visit(LockEvent lockEvent) {
            if (commitRequestId
                    .filter(requestId -> requestId.equals(lockEvent.lockToken().getRequestId()))
                    .isPresent()) {
                return ImmutableSet.of();
            } else {
                return lockEvent.lockDescriptors();
            }
        }

        @Override
        public Set<LockDescriptor> visit(UnlockEvent unlockEvent) {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return lockWatchCreatedEvent.lockDescriptors();
        }
    }
}
