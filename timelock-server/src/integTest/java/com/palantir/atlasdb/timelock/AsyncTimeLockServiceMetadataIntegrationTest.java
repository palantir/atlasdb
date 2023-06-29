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

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.ConjureIdentifiedVersion;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.LockWatchRequest;
import com.palantir.atlasdb.timelock.lock.AsyncLockService;
import com.palantir.atlasdb.timelock.lock.LockLog;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.client.IdentifiedLockRequest;
import com.palantir.lock.client.ImmutableIdentifiedLockRequest;
import com.palantir.lock.v2.LockResponseV2;
import com.palantir.lock.v2.LockResponseV2.Successful;
import com.palantir.lock.v2.LockResponseV2.Unsuccessful;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.ChangeMetadata;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockRequestMetadata;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchStateUpdate.Snapshot;
import com.palantir.lock.watch.LockWatchStateUpdate.Success;
import com.palantir.lock.watch.LockWatchStateUpdate.Visitor;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.timestamp.InMemoryTimestampService;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Test;

public class AsyncTimeLockServiceMetadataIntegrationTest {
    private static final String WATCHED_TABLE_NAME = "watched-table";
    private static final LockDescriptor WATCHED_LOCK_1 =
            AtlasRowLockDescriptor.of(WATCHED_TABLE_NAME, "lock1".getBytes(StandardCharsets.UTF_8));
    private static final LockDescriptor WATCHED_LOCK_2 =
            AtlasRowLockDescriptor.of(WATCHED_TABLE_NAME, "lock2".getBytes(StandardCharsets.UTF_8));
    private static final LockDescriptor WATCHED_LOCK_3 =
            AtlasRowLockDescriptor.of(WATCHED_TABLE_NAME, "lock3".getBytes(StandardCharsets.UTF_8));
    private static final LockDescriptor WATCHED_LOCK_4 =
            AtlasRowLockDescriptor.of(WATCHED_TABLE_NAME, "lock4".getBytes(StandardCharsets.UTF_8));
    private static final Map<LockDescriptor, ChangeMetadata> ALL_WATCHED_LOCKS_WITH_METADATA = ImmutableMap.of(
            WATCHED_LOCK_1,
            ChangeMetadata.unchanged(),
            WATCHED_LOCK_2,
            ChangeMetadata.updated("old".getBytes(StandardCharsets.UTF_8), "new".getBytes(StandardCharsets.UTF_8)),
            WATCHED_LOCK_3,
            ChangeMetadata.deleted("deleted".getBytes(StandardCharsets.UTF_8)),
            WATCHED_LOCK_4,
            ChangeMetadata.created("created".getBytes(StandardCharsets.UTF_8)));
    private static final String UNWATCHED_TABLE_NAME = "a-random-unwatched-table";
    private static final LockDescriptor UNWATCHED_LOCK_1 =
            AtlasRowLockDescriptor.of(UNWATCHED_TABLE_NAME, "lock1".getBytes(StandardCharsets.UTF_8));
    private static final IdentifiedLockRequest WATCHED_LOCK_REQUEST_WITH_METADATA =
            standardRequestWithMetadata(ALL_WATCHED_LOCKS_WITH_METADATA);

    private final LockLog lockLog = new LockLog(new MetricRegistry(), () -> 10000L);
    private final ScheduledExecutorService scheduledExecutorService = PTExecutors.newSingleThreadScheduledExecutor();
    private final AsyncLockService asyncLockService =
            AsyncLockService.createDefault(lockLog, scheduledExecutorService, scheduledExecutorService);
    private final AsyncTimelockServiceImpl timeLockService =
            new AsyncTimelockServiceImpl(asyncLockService, new InMemoryTimestampService(), lockLog);
    private final ConjureStartTransactionsRequest startTransactionsRequestWithInitialVersion =
            ConjureStartTransactionsRequest.builder()
                    .requestId(UUID.randomUUID())
                    .lastKnownVersion(ConjureIdentifiedVersion.of(
                            asyncLockService.leaderTime().id().id(), 0))
                    .numTransactions(1)
                    .requestorId(UUID.randomUUID())
                    .build();

    @Before
    public void setupLockServiceWithWatches() {
        timeLockService.startWatching(
                LockWatchRequest.of(ImmutableSet.of(LockWatchReferences.entireTable(WATCHED_TABLE_NAME))));
    }

    @Test
    public void lockEventContainsMetadataFromRequestIfWatched() {
        waitForFuture(timeLockService.lock(WATCHED_LOCK_REQUEST_WITH_METADATA));

        assertThat(getAllLockEventsMetadata())
                .containsExactly(Optional.of(LockRequestMetadata.of(ALL_WATCHED_LOCKS_WITH_METADATA)));
    }

    @Test
    public void lockEventDoesNotContainMetadataThatIsNotWatched() {

        IdentifiedLockRequest mixedRequest =
                standardRequestWithMetadata(ImmutableMap.<LockDescriptor, ChangeMetadata>builder()
                        .putAll(ALL_WATCHED_LOCKS_WITH_METADATA)
                        .put(
                                UNWATCHED_LOCK_1,
                                ChangeMetadata.updated(
                                        "bla".getBytes(StandardCharsets.UTF_8),
                                        "blabla".getBytes(StandardCharsets.UTF_8)))
                        .buildKeepingLast());
        waitForFuture(timeLockService.lock(mixedRequest));

        assertThat(getAllLockEventsMetadata())
                .containsExactly(Optional.of(LockRequestMetadata.of(ALL_WATCHED_LOCKS_WITH_METADATA)));
    }

    @Test
    public void noLockEventIsPublishedIfNothingIsWatched() {
        IdentifiedLockRequest mixedRequest = standardRequestWithMetadata(ImmutableMap.of(
                UNWATCHED_LOCK_1,
                ChangeMetadata.updated(
                        "bla".getBytes(StandardCharsets.UTF_8), "blabla".getBytes(StandardCharsets.UTF_8))));
        waitForFuture(timeLockService.lock(mixedRequest));
        assertThat(getAllLockEventsMetadata()).isEmpty();
    }

    @Test
    public void canRetrieveMultipleEventsWithMetadata() {
        List<Optional<LockRequestMetadata>> metadataList = new ArrayList<>();
        ALL_WATCHED_LOCKS_WITH_METADATA.forEach((lock, metadata) -> {
            Map<LockDescriptor, ChangeMetadata> map = ImmutableMap.of(lock, metadata);
            IdentifiedLockRequest request = standardRequestWithMetadata(map);
            waitForFuture(timeLockService.lock(request));
            metadataList.add(Optional.of(LockRequestMetadata.of(map)));
        });

        assertThat(getAllLockEventsMetadata()).containsExactlyElementsOf(metadataList);
    }

    @Test
    public void toleratesAndPassesDownAbsentMetadata() {
        // this will create a lock request for 4 lock descriptors, but with absent metadata
        IdentifiedLockRequest requestWithoutMetadata = ImmutableIdentifiedLockRequest.copyOf(
                        standardRequestWithMetadata(ALL_WATCHED_LOCKS_WITH_METADATA))
                .withMetadata(Optional.empty());
        waitForFuture(timeLockService.lock(requestWithoutMetadata));

        assertThat(getAllLockEventsMetadata()).containsExactly(Optional.empty());
    }

    @Test
    public void canMixWithOtherLockWatchEventsAndAbsentMetadata() {
        List<Optional<LockRequestMetadata>> metadataList = new ArrayList<>();
        ALL_WATCHED_LOCKS_WITH_METADATA.forEach((lock, metadata) -> {
            // -> LockWatchCreatedEvent
            timeLockService.startWatching(LockWatchRequest.of(
                    ImmutableSet.of(LockWatchReferences.entireTable("randomTable" + metadataList.size()))));

            // -> LockEvent with metadata
            Map<LockDescriptor, ChangeMetadata> map = ImmutableMap.of(lock, metadata);
            IdentifiedLockRequest requestWithMetadata = standardRequestWithMetadata(map);
            LockResponseV2 response = waitForFuture(timeLockService.lock(requestWithMetadata));
            metadataList.add(Optional.of(LockRequestMetadata.of(map)));

            // -> UnlockEvent
            waitForFuture(timeLockService.unlock(ImmutableSet.of(getToken(response))));

            // -> LockEvent with absent metadata
            IdentifiedLockRequest requestWithoutMetadata = ImmutableIdentifiedLockRequest.copyOf(
                            standardRequestWithMetadata(map))
                    .withMetadata(Optional.empty());
            response = waitForFuture(timeLockService.lock(requestWithoutMetadata));
            metadataList.add(Optional.empty());

            // -> UnlockEvent
            waitForFuture(timeLockService.unlock(ImmutableSet.of(getToken(response))));
        });

        assertThat(getAllLockWatchEvents()).hasSize(5 * ALL_WATCHED_LOCKS_WITH_METADATA.size());
        assertThat(getAllLockEventsMetadata()).containsExactlyElementsOf(metadataList);
    }

    private List<Optional<LockRequestMetadata>> getAllLockEventsMetadata() {
        return getAllLockWatchEvents().stream()
                .map(event -> event.accept(new LockWatchEvent.Visitor<Optional<LockEvent>>() {
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
                }))
                .flatMap(Optional::stream)
                .map(LockEvent::metadata)
                .collect(Collectors.toList());
    }

    private List<LockWatchEvent> getAllLockWatchEvents() {
        ConjureStartTransactionsResponse response =
                waitForFuture(timeLockService.startTransactionsWithWatches(startTransactionsRequestWithInitialVersion));

        return response.getLockWatchUpdate().accept(new Visitor<>() {
            @Override
            public List<LockWatchEvent> visit(Success success) {
                return success.events();
            }

            @Override
            public List<LockWatchEvent> visit(Snapshot snapshot) {
                return ImmutableList.of();
            }
        });
    }

    private static <T> T waitForFuture(Future<T> future) {
        Awaitility.await().atMost(Duration.ofMillis(200)).until(future::isDone);
        try {
            return future.get();
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    private static LockToken getToken(LockResponseV2 response) {
        return response.accept(new LockResponseV2.Visitor<Optional<LockToken>>() {
                    @Override
                    public Optional<LockToken> visit(Successful successful) {
                        return Optional.of(successful.getToken());
                    }

                    @Override
                    public Optional<LockToken> visit(Unsuccessful failure) {
                        return Optional.empty();
                    }
                })
                .orElseThrow();
    }

    private static IdentifiedLockRequest standardRequestWithMetadata(Map<LockDescriptor, ChangeMetadata> metadata) {
        return IdentifiedLockRequest.of(metadata.keySet(), 1000, "testClient", LockRequestMetadata.of(metadata));
    }
}
