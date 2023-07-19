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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.watch.TimestampStateStore.CommitInfo;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.client.LeasedLockToken;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.ChangeMetadata;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.ImmutableTransactionsLockWatchUpdate;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.immutables.value.Value;

/**
 * Encapsulates a response from the event log, containing both a list of events relevant to those requested, as well
 * information around whether it is necessary to clear cache upstream.
 *
 * It is important to note that the additional logic in this class does *not* change any of the state here, and instead
 * just performs additional verification (mainly around confirming that the events contained here span a sufficient
 * version range). Events are never filtered here - while those passed in should be minimal, it is the responsibility
 * of the consumer of {@link TransactionsLockWatchUpdate} or {@link CommitUpdate} to filter out additional events.
 */
@Value.Immutable
interface ClientLogEvents {
    SafeLogger log = SafeLoggerFactory.get(ClientLogEvents.class);

    LockWatchEvents events();

    boolean clearCache();

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
         Case 5: the client is very far behind, but still has a version. In this case, we should not check based on the
                 client version, but rather the lower bound of the timestamp mapping.
        */
        verifyReturnedEventsEnclosesTransactionVersions(
                lastKnownVersion
                        .filter(_unused -> !clearCache())
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

    default CommitUpdate toCommitUpdate(
            LockWatchVersion startVersion, LockWatchVersion endVersion, Optional<CommitInfo> commitInfo) {
        if (clearCache()) {
            return CommitUpdate.invalidateAll();
        }

        // We want to ensure that we do not miss any versions, but we do not care about the event with the same version
        // as the start version.
        verifyReturnedEventsEnclosesTransactionVersions(startVersion.version() + 1, endVersion.version());

        LockEventVisitor eventVisitor = new LockEventVisitor(commitInfo.map(CommitInfo::commitLockToken));

        Set<LockDescriptor> locksFromLockWatchCreatedEvents = events().events().stream()
                .flatMap(event -> event.accept(LockWatchCreatedEventVisitor.INSTANCE).stream())
                .collect(Collectors.toUnmodifiableSet());
        List<LockEvent> lockEvents = events().events().stream()
                .flatMap(event -> event.accept(eventVisitor).stream())
                .collect(Collectors.toUnmodifiableList());
        Set<LockDescriptor> locksFromLockEvents = lockEvents.stream()
                .map(LockEvent::lockDescriptors)
                .flatMap(Set::stream)
                .collect(Collectors.toUnmodifiableSet());

        /*
         * Metadata aggregation semantics for a lock descriptor L:
         * - If L appears in a LockWatchCreatedEvent, L will be excluded from the aggregation result.
         * - Otherwise, if there is a lock event that contains L, but L does not have metadata associated in this lock
         *   event, L is also excluded from the aggregation result.
         * - Otherwise, the metadata objects associated with L are listed in the order of the lock events they appear
         *   in.
         */
        Map<LockDescriptor, ImmutableList.Builder<ChangeMetadata>> locksWithAggregatedMetadataBuilder = KeyedStream.of(
                        Sets.difference(locksFromLockEvents, locksFromLockWatchCreatedEvents).stream())
                .map(_unused -> new ImmutableList.Builder<ChangeMetadata>())
                .collectToMap();
        for (LockEvent lockEvent : lockEvents) {
            for (LockDescriptor lock : lockEvent.lockDescriptors()) {
                if (!locksWithAggregatedMetadataBuilder.containsKey(lock)) {
                    continue;
                }
                Optional<ChangeMetadata> changeMetadata = lockEvent
                        .metadata()
                        .map(metadata ->
                                metadata.lockDescriptorToChangeMetadata().get(lock));
                if (lockEvent.metadata().isEmpty() || changeMetadata.isEmpty()) {
                    locksWithAggregatedMetadataBuilder.remove(lock);
                } else {
                    locksWithAggregatedMetadataBuilder.get(lock).add(changeMetadata.get());
                }
            }
        }

        Set<LockDescriptor> locksTakenOut = Sets.union(locksFromLockEvents, locksFromLockWatchCreatedEvents);
        Map<LockDescriptor, List<ChangeMetadata>> locksWithAggregatedMetadata = KeyedStream.ofEntries(
                        locksWithAggregatedMetadataBuilder.entrySet().stream())
                .<List<ChangeMetadata>>map(ImmutableList.Builder::build)
                .collectToMap();
        return CommitUpdate.invalidateSome(locksTakenOut, locksWithAggregatedMetadata);
    }

    default void verifyReturnedEventsEnclosesTransactionVersions(long lowerBound, long upperBound) {
        if (lowerBound > upperBound) {
            return;
        }

        Range<Long> rangeToTest = Range.closed(lowerBound, upperBound);
        events().versionRange().ifPresent(eventsRange -> {
            if (!eventsRange.encloses(rangeToTest)) {
                log.warn(
                        "Events do not enclose the required version",
                        SafeArg.of("lowerBound", lowerBound),
                        SafeArg.of("upperBound", upperBound));
                throw new TransactionLockWatchFailedException("Events do not enclose the required versions");
            }
        });
    }

    static ImmutableClientLogEvents.Builder builder() {
        return ImmutableClientLogEvents.builder();
    }

    enum LockWatchCreatedEventVisitor implements LockWatchEvent.Visitor<Set<LockDescriptor>> {
        INSTANCE;

        @Override
        public Set<LockDescriptor> visit(LockEvent lockEvent) {
            return ImmutableSet.of();
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

    final class LockEventVisitor implements LockWatchEvent.Visitor<Optional<LockEvent>> {
        private final Optional<UUID> commitRequestId;

        private LockEventVisitor(Optional<LockToken> commitLocksToken) {
            commitRequestId = commitLocksToken
                    .filter(lockToken -> lockToken instanceof LeasedLockToken)
                    .map(lockToken ->
                            ((LeasedLockToken) lockToken).serverToken().getRequestId());
        }

        @Override
        public Optional<LockEvent> visit(LockEvent lockEvent) {
            if (commitRequestId
                    .filter(requestId -> requestId.equals(lockEvent.lockToken().getRequestId()))
                    .isPresent()) {
                return Optional.empty();
            } else {
                return Optional.of(lockEvent);
            }
        }

        @Override
        public Optional<LockEvent> visit(UnlockEvent unlockEvent) {
            return Optional.empty();
        }

        @Override
        public Optional<LockEvent> visit(LockWatchCreatedEvent lockWatchCreatedEvent) {
            return Optional.empty();
        }
    }
}
