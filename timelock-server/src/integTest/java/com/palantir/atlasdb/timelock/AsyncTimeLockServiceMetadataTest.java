/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatException;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.futures.AtlasFutures;
import com.palantir.atlasdb.timelock.api.ConjureIdentifiedVersion;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.LockWatchRequest;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.atlasdb.timelock.metrics.MetadataMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.client.ImmutableIdentifiedLockRequest;
import com.palantir.lock.watch.ChangeMetadata;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchStateUpdate.Snapshot;
import com.palantir.lock.watch.LockWatchStateUpdate.Success;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.timestamp.InMemoryTimestampService;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;

public class AsyncTimeLockServiceMetadataTest {
    private static final LockWatchEventVisitor LOCK_WATCH_EVENT_VISITOR = new LockWatchEventVisitor();
    private static final LockWatchStateUpdateVisitor LOCK_WATCH_STATE_UPDATE_VISITOR =
            new LockWatchStateUpdateVisitor();

    private static final String WATCHED_TABLE_NAME = "watched-table";
    private static final LockDescriptor WATCHED_LOCK =
            AtlasRowLockDescriptor.of(WATCHED_TABLE_NAME, PtBytes.toBytes("lock1"));
    private static final Map<LockDescriptor, ChangeMetadata> ALL_WATCHED_LOCKS_WITH_METADATA =
            ImmutableMap.of(WATCHED_LOCK, ChangeMetadata.updated(PtBytes.toBytes("old"), PtBytes.toBytes("new")));
    private static final String UNWATCHED_TABLE_NAME = "a-random-unwatched-table";
    private static final LockDescriptor UNWATCHED_LOCK =
            AtlasRowLockDescriptor.of(UNWATCHED_TABLE_NAME, PtBytes.toBytes("lock1"));
    private static final IdentifiedLockRequest WATCHED_LOCK_REQUEST_WITH_METADATA =
            standardRequestWithMetadata(ALL_WATCHED_LOCKS_WITH_METADATA);

    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final MetadataMetrics metadataMetrics = MetadataMetrics.of(metricsManager.getTaggedRegistry());
    private final LockLog lockLog = new LockLog(metricsManager.getRegistry(), () -> 10000L);
    private final ScheduledExecutorService scheduledExecutorService = new DeterministicScheduler();
    private final AsyncLockService asyncLockService =
            AsyncLockService.createDefault(lockLog, scheduledExecutorService, scheduledExecutorService);
    private final AsyncTimelockServiceImpl timeLockService =
            new AsyncTimelockServiceImpl(asyncLockService, new InMemoryTimestampService(), lockLog, metadataMetrics);
    private final ConjureStartTransactionsRequest startTransactionsRequestWithInitialVersion =
            ConjureStartTransactionsRequest.builder()
                    .requestId(UUID.randomUUID())
                    .lastKnownVersion(ConjureIdentifiedVersion.of(
                            asyncLockService.leaderTime().id().id(), 0))
                    .numTransactions(1)
                    .requestorId(UUID.randomUUID())
                    .build();

    @Before
    public void setup() {
        timeLockService.startWatching(
                LockWatchRequest.of(ImmutableSet.of(LockWatchReferences.entireTable(WATCHED_TABLE_NAME))));
    }

    @Test
    public void absentMetadataIsPassedThrough() {
        // this will create a lock request for 4 lock descriptors, but with absent metadata
        IdentifiedLockRequest requestWithoutMetadata = ImmutableIdentifiedLockRequest.copyOf(
                        standardRequestWithMetadata(ALL_WATCHED_LOCKS_WITH_METADATA))
                .withMetadata(Optional.empty());
        timeLockService.lock(requestWithoutMetadata);

        assertThat(getAllLockEventsMetadata()).containsExactly(Optional.empty());
    }

    @Test
    public void metadataIsPassedThroughForWatchedTable() {
        timeLockService.lock(WATCHED_LOCK_REQUEST_WITH_METADATA);

        assertThat(getAllLockEventsMetadata())
                .containsExactly(Optional.of(LockRequestMetadata.of(ALL_WATCHED_LOCKS_WITH_METADATA)));
    }

    // This test is trivial for our current metadata constraints (metadata is only allowed for locks
    // that are part of the original request), but we would also want this behavior if the constraints weren't there
    @Test
    public void noLockEventIsPublishedIfNothingIsWatched() {
        IdentifiedLockRequest mixedRequest = standardRequestWithMetadata(ImmutableMap.of(
                UNWATCHED_LOCK, ChangeMetadata.updated(PtBytes.toBytes("bla"), PtBytes.toBytes("blabla"))));
        timeLockService.lock(mixedRequest);

        assertThat(getAllLockEventsMetadata()).isEmpty();
    }

    // This test verifies that future developers are alerted when they break the metadata invariant by trying to pass
    // metadata that is not attached to a lock descriptor that is part of the original request.
    @Test
    public void cannotPassDownMetadataForUnknownLockDescriptors() {
        IdentifiedLockRequest goodRequest =
                standardRequestWithMetadata(ImmutableMap.of(WATCHED_LOCK, ChangeMetadata.unchanged()));
        IdentifiedLockRequest badRequest = ImmutableIdentifiedLockRequest.builder()
                .from(goodRequest)
                .metadata(LockRequestMetadata.of(
                        ImmutableMap.of(StringLockDescriptor.of("some-random-lock"), ChangeMetadata.unchanged())))
                .build();
        assertThatException()
                .isThrownBy(() -> timeLockService.lock(badRequest).get())
                .isInstanceOf(ExecutionException.class)
                .havingCause()
                .isInstanceOf(AssertionError.class)
                .withMessage("Unknown lock descriptor in metadata");
    }

    @Test
    public void updatesMetadataSizeMetricForAllMetadata() {
        // empty metadata (but not absent)
        timeLockService.lock(standardRequestWithMetadata(ImmutableMap.of()));
        timeLockService.lock(standardRequestWithMetadata(ImmutableMap.of(
                StringLockDescriptor.of("1"), ChangeMetadata.unchanged(),
                StringLockDescriptor.of("2"), ChangeMetadata.unchanged(),
                StringLockDescriptor.of("3"), ChangeMetadata.unchanged())));
        // absent metadata
        timeLockService.lock(ImmutableIdentifiedLockRequest.copyOf(WATCHED_LOCK_REQUEST_WITH_METADATA)
                .withMetadata(Optional.empty()));
        assertThat(metadataMetrics.requestChangeMetadataSize().getSnapshot().getValues())
                .containsOnly(0, 3, 0);
    }

    private List<Optional<LockRequestMetadata>> getAllLockEventsMetadata() {
        return getAllLockWatchEvents().stream()
                .map(event -> event.accept(LOCK_WATCH_EVENT_VISITOR))
                .flatMap(Optional::stream)
                .map(LockEvent::metadata)
                .collect(Collectors.toList());
    }

    private List<LockWatchEvent> getAllLockWatchEvents() {
        ListenableFuture<ConjureStartTransactionsResponse> responseFuture =
                timeLockService.startTransactionsWithWatches(startTransactionsRequestWithInitialVersion);
        return AtlasFutures.getUnchecked(responseFuture).getLockWatchUpdate().accept(LOCK_WATCH_STATE_UPDATE_VISITOR);
    }

    private static IdentifiedLockRequest standardRequestWithMetadata(Map<LockDescriptor, ChangeMetadata> metadata) {
        return IdentifiedLockRequest.of(metadata.keySet(), 1000, "testClient", LockRequestMetadata.of(metadata));
    }

    private static final class LockWatchEventVisitor implements LockWatchEvent.Visitor<Optional<LockEvent>> {
        @Override
        public Optional<LockEvent> visit(LockEvent lockEvent) {
            return Optional.of(lockEvent);
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

    private static final class LockWatchStateUpdateVisitor
            implements LockWatchStateUpdate.Visitor<List<LockWatchEvent>> {
        @Override
        public List<LockWatchEvent> visit(Success success) {
            return success.events();
        }

        @Override
        public List<LockWatchEvent> visit(Snapshot snapshot) {
            return ImmutableList.of();
        }
    }
}
