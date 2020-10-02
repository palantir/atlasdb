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

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.immutables.value.Value;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.palantir.atlasdb.keyvalue.api.watch.TimestampStateStore.CommitInfo;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.LockDescriptor;
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

@Value.Immutable
interface ClientLogEvents {

    LockWatchEvents events();

    boolean clearCache();

    @Value.Derived
    default Optional<Long> latestVersion() {
        return events().versionRange().map(Range::upperEndpoint);
    }

    default TransactionsLockWatchUpdate toTransactionsLockWatchUpdate(
            TimestampMapping timestampMapping,
            Optional<LockWatchVersion> lastKnownVersion) {
        // If the client is at the same version as the earliest version in the timestamp mapping, then they will
        // only receive versions after that - and therefore the range of versions coming back from the events will not
        // enclose the versions in the mapping. This flag makes sure that we don't throw on this case
        boolean offsetStartVersion = lastKnownVersion.map(
                version -> version.version() == timestampMapping.versionRange().lowerEndpoint()).orElse(false);
        verifyReturnedEventsEnclosesTransactionVersions(timestampMapping.versionRange(), offsetStartVersion);
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

        verifyReturnedEventsEnclosesTransactionVersions(
                Range.closed(startVersion.version(), commitInfo.commitVersion().version()), true);

        LockEventVisitor eventVisitor = new LockEventVisitor(commitInfo.commitLockToken());
        Set<LockDescriptor> locksTakenOut = new HashSet<>();
        events().events().forEach(event -> locksTakenOut.addAll(event.accept(eventVisitor)));
        return ImmutableInvalidateSome.builder().invalidatedLocks(locksTakenOut).build();
    }

    default void verifyReturnedEventsEnclosesTransactionVersions(Range<Long> versionRange, boolean offsetStartVersion) {
        // If we offset the start version, but the range is already [x..x], we throw when creating the range.
        if (versionRange.lowerEndpoint() == versionRange.upperEndpoint() && offsetStartVersion) {
            return;
        }

        Range<Long> rangeToTest;
        if (offsetStartVersion) {
            rangeToTest = Range.closed(versionRange.lowerEndpoint() + 1, versionRange.upperEndpoint());
        } else {
            rangeToTest = versionRange;
        }

        events().versionRange().ifPresent(eventsRange -> {
            if (!eventsRange.encloses(rangeToTest)) {
                throw new TransactionLockWatchFailedException("Events do not enclose the required versions");
            }
        });
    }

    class Builder extends ImmutableClientLogEvents.Builder {}

    final class LockEventVisitor implements LockWatchEvent.Visitor<Set<LockDescriptor>> {
        private final LockToken commitLocksToken;

        private LockEventVisitor(LockToken commitLocksToken) {
            this.commitLocksToken = commitLocksToken;
        }

        @Override
        public Set<LockDescriptor> visit(LockEvent lockEvent) {
            if (lockEvent.lockToken().equals(commitLocksToken)) {
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
