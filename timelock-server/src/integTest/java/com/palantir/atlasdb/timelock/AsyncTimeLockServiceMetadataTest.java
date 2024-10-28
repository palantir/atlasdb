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
import static org.assertj.core.api.Assertions.fail;

import com.codahale.metrics.MetricRegistry;
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
import com.palantir.atlasdb.timelock.lockwatches.BufferMetrics;
import com.palantir.atlasdb.timelock.lockwatches.RequestMetrics;
import com.palantir.atlasdb.timelock.timestampleases.TimestampLeaseMetrics;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.client.ImmutableIdentifiedLockRequest;
import com.palantir.lock.watch.ChangeMetadata;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.timestamp.InMemoryTimestampService;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AsyncTimeLockServiceMetadataTest {
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

    private final LockLog lockLog = new LockLog(new MetricRegistry(), () -> 10000L);
    private final ScheduledExecutorService scheduledExecutorService = new DeterministicScheduler();

    private final MetricsManager metricsManager = MetricsManagers.createForTests();
    private final RequestMetrics requestMetrics = RequestMetrics.of(metricsManager.getTaggedRegistry());
    private final AsyncLockService asyncLockService = AsyncLockService.createDefault(
            lockLog,
            scheduledExecutorService,
            scheduledExecutorService,
            BufferMetrics.of(metricsManager.getTaggedRegistry()),
            TimestampLeaseMetrics.of(metricsManager.getTaggedRegistry()));
    private final AsyncTimelockServiceImpl timeLockService =
            new AsyncTimelockServiceImpl(asyncLockService, new InMemoryTimestampService(), lockLog, requestMetrics);
    private final ConjureStartTransactionsRequest startTransactionsRequestWithInitialVersion =
            ConjureStartTransactionsRequest.builder()
                    .requestId(UUID.randomUUID())
                    .lastKnownVersion(ConjureIdentifiedVersion.of(
                            asyncLockService.leaderTime().id().id(), 0))
                    .numTransactions(1)
                    .requestorId(UUID.randomUUID())
                    .build();

    @BeforeEach
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
    public void updatesRequestMetricsCorrectly() {
        timeLockService.lock(standardRequestWithMetadata(ImmutableMap.of()));
        timeLockService.lock(standardRequestWithMetadata(ImmutableMap.of(
                StringLockDescriptor.of("1"), ChangeMetadata.unchanged(),
                StringLockDescriptor.of("2"), ChangeMetadata.unchanged(),
                StringLockDescriptor.of("3"), ChangeMetadata.unchanged())));
        timeLockService.lock(ImmutableIdentifiedLockRequest.copyOf(WATCHED_LOCK_REQUEST_WITH_METADATA)
                .withMetadata(Optional.empty()));

        assertThat(requestMetrics.changeMetadata().getSnapshot().getValues()).containsOnly(0, 3, 0);
    }

    private List<Optional<LockRequestMetadata>> getAllLockEventsMetadata() {
        return LockWatchIntegrationTestUtilities.extractMetadata(getAllLockWatchEvents());
    }

    private List<LockWatchEvent> getAllLockWatchEvents() {
        ListenableFuture<ConjureStartTransactionsResponse> responseFuture =
                timeLockService.startTransactionsWithWatches(startTransactionsRequestWithInitialVersion);
        return AtlasFutures.getUnchecked(responseFuture)
                .getLockWatchUpdate()
                .accept(AssertSuccessVisitor.INSTANCE)
                .events();
    }

    private static IdentifiedLockRequest standardRequestWithMetadata(Map<LockDescriptor, ChangeMetadata> metadata) {
        return IdentifiedLockRequest.of(metadata.keySet(), 1000, "testClient", LockRequestMetadata.of(metadata));
    }

    private enum AssertSuccessVisitor implements LockWatchStateUpdate.Visitor<LockWatchStateUpdate.Success> {
        INSTANCE;

        @Override
        public LockWatchStateUpdate.Success visit(LockWatchStateUpdate.Success success) {
            return success;
        }

        @Override
        public LockWatchStateUpdate.Success visit(LockWatchStateUpdate.Snapshot snapshot) {
            return fail("Unexpected snapshot");
        }
    }
}
