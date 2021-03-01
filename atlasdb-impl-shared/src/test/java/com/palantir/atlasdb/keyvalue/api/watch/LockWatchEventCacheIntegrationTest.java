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

import static com.palantir.logsafe.testing.Assertions.assertThat;
import static com.palantir.logsafe.testing.Assertions.assertThatLoggableExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.common.time.NanoTime;
import com.palantir.lock.AtlasRowLockDescriptor;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.client.LeasedLockTokenCreator;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.Lease;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.ImmutableTransactionUpdate;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchEventCache;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.NoOpLockWatchEventCache;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

public class LockWatchEventCacheIntegrationTest {
    private static final String TABLE = "table";
    // Serializes as "dGFibGUAAQ=="
    private static final LockDescriptor DESCRIPTOR = AtlasRowLockDescriptor.of(TABLE, new byte[] {1});
    // Serializes as "dGFibGUAAg=="
    private static final LockDescriptor DESCRIPTOR_2 = AtlasRowLockDescriptor.of(TABLE, new byte[] {2});
    // Serializes as "dGFibGUAAw=="
    private static final LockDescriptor DESCRIPTOR_3 = AtlasRowLockDescriptor.of(TABLE, new byte[] {3});
    private static final LockWatchReferences.LockWatchReference REFERENCE = LockWatchReferences.entireTable("table");
    private static final UUID COMMIT_UUID = UUID.fromString("203fcd7a-b3d7-4c2a-9d2c-3d61cde1ba59");
    private static final LockToken COMMIT_TOKEN = LockToken.of(COMMIT_UUID);

    private static final LockWatchEvent WATCH_EVENT = LockWatchCreatedEvent.builder(
                    ImmutableSet.of(REFERENCE), ImmutableSet.of(DESCRIPTOR))
            .build(4L);
    private static final LockWatchEvent UNLOCK_EVENT =
            UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR_2)).build(5L);
    private static final LockWatchEvent LOCK_EVENT =
            LockEvent.builder(ImmutableSet.of(DESCRIPTOR_3), COMMIT_TOKEN).build(6L);
    private static final UUID EVENT2_UUID = UUID.fromString("888fcd7a-b3d7-4d2a-9d2c-3d61cde1ba44");
    private static final LockWatchEvent LOCK_EVENT_2 = LockEvent.builder(
                    ImmutableSet.of(DESCRIPTOR), LockToken.of(EVENT2_UUID))
            .build(7L);

    private static final UUID LEADER = UUID.fromString("470c855e-f77b-44df-b56a-14d3df085dbc");
    private static final LockWatchStateUpdate.Success SUCCESS_2 =
            LockWatchStateUpdate.success(LEADER, 7L, ImmutableList.of(LOCK_EVENT_2));
    private static final LockWatchStateUpdate SNAPSHOT =
            LockWatchStateUpdate.snapshot(LEADER, 3L, ImmutableSet.of(DESCRIPTOR_2), ImmutableSet.of());
    private static final long SUCCESS_VERSION = 6L;
    private static final LockWatchStateUpdate SUCCESS = LockWatchStateUpdate.success(
            LEADER, SUCCESS_VERSION, ImmutableList.of(WATCH_EVENT, UNLOCK_EVENT, LOCK_EVENT));
    private static final long START_TS_1 = 1L;
    private static final long START_TS_2 = 16L;
    private static final long START_TS_3 = 25L;
    private static final ImmutableSet<TransactionUpdate> COMMIT_UPDATE =
            ImmutableSet.of(ImmutableTransactionUpdate.builder()
                    .startTs(START_TS_1)
                    .commitTs(5L)
                    .writesToken(COMMIT_TOKEN)
                    .build());
    private static final ImmutableSet<Long> TIMESTAMPS = ImmutableSet.of(START_TS_1);
    private static final ImmutableSet<Long> TIMESTAMPS_2 = ImmutableSet.of(START_TS_2);
    private static final String BASE = "src/test/resources/lockwatch-event-cache-output/";
    private static final Mode MODE = Mode.CI;

    private enum Mode {
        DEV,
        CI;

        boolean isDev() {
            return this.equals(Mode.DEV);
        }
    }

    private LockWatchEventCacheImpl realEventCache;
    private LockWatchEventCache fakeCache;
    private LockWatchEventCache eventCache;
    private int part;

    @Rule
    public TestName name = new TestName();

    @Before
    public void before() {
        createEventCache(5);
        part = 1;
    }

    private void verifyStage() {
        ObjectMapper mapper = new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .registerModule(new Jdk8Module())
                .registerModule(new GuavaModule());
        try {
            Path path = Paths.get(BASE + name.getMethodName() + "/event-cache-" + part + ".json");
            LockWatchEventCacheState eventCacheState = realEventCache.getStateForTesting();

            if (MODE.isDev()) {
                mapper.writeValue(path.toFile(), eventCacheState);
            } else {
                String ourJson = mapper.writeValueAsString(eventCacheState);
                String theirJson = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
                assertThat(ourJson).isEqualTo(theirJson);
            }
            part++;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Test
    public void emptySuccessesFollowingSnapshotsDoNotCauseAdditionalCacheClearance() {
        LockWatchStateUpdate snapshot =
                LockWatchStateUpdate.snapshot(LEADER, 3L, ImmutableSet.of(DESCRIPTOR_2), ImmutableSet.of());
        LockWatchStateUpdate emptySuccess = LockWatchStateUpdate.success(LEADER, 3L, ImmutableList.of());
        LockWatchEvent lockEvent =
                LockEvent.builder(ImmutableSet.of(DESCRIPTOR_3), COMMIT_TOKEN).build(4L);
        LockWatchStateUpdate success = LockWatchStateUpdate.success(LEADER, 4L, ImmutableList.of(lockEvent));

        setupInitialState();
        verifyStage();

        eventCache.processStartTransactionsUpdate(ImmutableSet.of(1L), snapshot);
        verifyStage();

        assertThat(eventCache
                        .getUpdateForTransactions(ImmutableSet.of(1L), Optional.of(LockWatchVersion.of(LEADER, 2L)))
                        .clearCache())
                .isTrue();
        assertThat(eventCache
                        .getUpdateForTransactions(ImmutableSet.of(1L), Optional.of(LockWatchVersion.of(LEADER, 3L)))
                        .clearCache())
                .isFalse();

        eventCache.processStartTransactionsUpdate(ImmutableSet.of(2L), emptySuccess);
        verifyStage();

        assertThat(eventCache
                        .getUpdateForTransactions(ImmutableSet.of(2L), Optional.of(LockWatchVersion.of(LEADER, 3L)))
                        .clearCache())
                .isFalse();

        eventCache.processStartTransactionsUpdate(ImmutableSet.of(3L), emptySuccess);
        verifyStage();

        assertThat(eventCache
                        .getUpdateForTransactions(ImmutableSet.of(3L), Optional.of(LockWatchVersion.of(LEADER, 3L)))
                        .clearCache())
                .isFalse();

        eventCache.processStartTransactionsUpdate(ImmutableSet.of(99L), success);
        verifyStage();

        assertThat(eventCache
                        .getUpdateForTransactions(ImmutableSet.of(99L), Optional.of(LockWatchVersion.of(LEADER, 3L)))
                        .events())
                .containsExactly(lockEvent);
        assertThat(eventCache
                        .getUpdateForTransactions(ImmutableSet.of(99L), Optional.of(LockWatchVersion.of(LEADER, 4L)))
                        .events())
                .isEmpty();
    }

    @Test
    public void processStartTimestampUpdateOnMultipleBatches() {
        setupInitialState();
        verifyStage();

        Set<Long> secondTimestamps = ImmutableSet.of(5L, 123L);
        eventCache.processStartTransactionsUpdate(secondTimestamps, SUCCESS);
        verifyStage();

        Set<Long> thirdTimestamps = ImmutableSet.of(6677L, 8888L);
        eventCache.processStartTransactionsUpdate(thirdTimestamps, SUCCESS_2);
        verifyStage();
    }

    @Test
    public void sameUpdateTwiceButDifferentTimestamps() {
        setupInitialState();
        Set<Long> secondTimestamps = ImmutableSet.of(11L, 12L);
        Set<Long> thirdTimestamps = ImmutableSet.of(91L, 92L, 93L);

        eventCache.processStartTransactionsUpdate(secondTimestamps, SUCCESS);
        verifyStage();

        eventCache.processStartTransactionsUpdate(thirdTimestamps, SUCCESS);
        verifyStage();
    }

    @Test
    public void smallerUpdateAfterLargeUpdateDoesNotAffectCache() {
        setupInitialState();
        Set<Long> secondTimestamps = ImmutableSet.of(11L, 12L);
        Set<Long> thirdTimestamps = ImmutableSet.of(91L, 92L, 93L);

        eventCache.processStartTransactionsUpdate(
                secondTimestamps,
                LockWatchStateUpdate.success(
                        LEADER, 7L, ImmutableList.of(WATCH_EVENT, UNLOCK_EVENT, LOCK_EVENT, LOCK_EVENT_2)));
        verifyStage();

        eventCache.processStartTransactionsUpdate(
                thirdTimestamps, LockWatchStateUpdate.success(LEADER, 5L, ImmutableList.of(WATCH_EVENT, UNLOCK_EVENT)));
        verifyStage();
    }

    @Test
    public void largerUpdateAfterSmallUpdateOnlyPicksUpNewEvents() {
        setupInitialState();
        Set<Long> secondTimestamps = ImmutableSet.of(11L, 12L);
        Set<Long> thirdTimestamps = ImmutableSet.of(91L, 92L, 93L);

        eventCache.processStartTransactionsUpdate(
                secondTimestamps,
                LockWatchStateUpdate.success(LEADER, 5L, ImmutableList.of(WATCH_EVENT, UNLOCK_EVENT)));
        verifyStage();

        eventCache.processStartTransactionsUpdate(
                thirdTimestamps,
                LockWatchStateUpdate.success(
                        LEADER, 7L, ImmutableList.of(WATCH_EVENT, UNLOCK_EVENT, LOCK_EVENT, LOCK_EVENT_2)));
        verifyStage();
    }

    @Test
    public void getCommitUpdateIsInvalidatedAllIfEventsHaveBeenDeleted() {
        createEventCache(2);
        setupInitialState();
        eventCache.processGetCommitTimestampsUpdate(COMMIT_UPDATE, SUCCESS);
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), SUCCESS_2);
        verifyStage();

        CommitUpdate commitUpdate = eventCache.getCommitUpdate(START_TS_1);
        assertThat(commitUpdate.accept(new InvalidatedAllVisitor())).isTrue();
    }

    @Test
    public void cacheClearedOnSnapshotUpdate() {
        setupInitialState();
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_2, SUCCESS);
        verifyStage();

        LockWatchStateUpdate snapshot2 =
                LockWatchStateUpdate.snapshot(LEADER, 7L, ImmutableSet.of(DESCRIPTOR), ImmutableSet.of());
        Set<Long> timestamps3 = ImmutableSet.of(123L, 1255L);
        eventCache.processStartTransactionsUpdate(timestamps3, snapshot2);
        verifyStage();
    }

    @Test
    public void getEventsForTransactionsNoCondensing() {
        setupInitialState();
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), SUCCESS);
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_2, SUCCESS_2);
        verifyStage();

        // Client is behind and needs a snapshot, but no compression can be done as the first transaction is at the
        // first version
        TransactionsLockWatchUpdate results =
                eventCache.getUpdateForTransactions(Sets.union(TIMESTAMPS, TIMESTAMPS_2), Optional.empty());
        assertThat(results.clearCache()).isTrue();
        assertThat(results.startTsToSequence())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        START_TS_1, LockWatchVersion.of(LEADER, 3L), START_TS_2, LockWatchVersion.of(LEADER, 7L)));
        assertThat(results.events())
                .containsExactly(
                        LockWatchCreatedEvent.builder(ImmutableSet.of(), ImmutableSet.of(DESCRIPTOR_2))
                                .build(3L),
                        WATCH_EVENT,
                        UNLOCK_EVENT,
                        LOCK_EVENT,
                        LOCK_EVENT_2);
    }

    @Test
    public void getEventsForTransactionsSomeCondensing() {
        setupInitialState();
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_2, SUCCESS);
        verifyStage();

        eventCache.processStartTransactionsUpdate(ImmutableSet.of(START_TS_3), SUCCESS_2);
        verifyStage();

        // Client is behind and needs a snapshot, and some but not all events will be condensed
        TransactionsLockWatchUpdate results =
                eventCache.getUpdateForTransactions(ImmutableSet.of(START_TS_2, START_TS_3), Optional.empty());
        assertThat(results.clearCache()).isTrue();
        assertThat(results.startTsToSequence())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        START_TS_2, LockWatchVersion.of(LEADER, 6L), START_TS_3, LockWatchVersion.of(LEADER, 7L)));
        assertThat(results.events())
                .containsExactly(
                        LockWatchCreatedEvent.builder(
                                        ImmutableSet.of(REFERENCE), ImmutableSet.of(DESCRIPTOR, DESCRIPTOR_3))
                                .build(6L),
                        LOCK_EVENT_2);
    }

    @Test
    public void getEventsForTransactionsMaxCondensing() {
        setupInitialState();
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), SUCCESS);
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_2, SUCCESS_2);
        verifyStage();

        // Client is behind, and all events will be condensed into a single snapshot and no other events
        TransactionsLockWatchUpdate results = eventCache.getUpdateForTransactions(TIMESTAMPS_2, Optional.empty());
        assertThat(results.clearCache()).isTrue();
        assertThat(results.startTsToSequence())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(START_TS_2, LockWatchVersion.of(LEADER, 7L)));
        assertThat(results.events())
                .containsExactly(LockWatchCreatedEvent.builder(
                                ImmutableSet.of(REFERENCE), ImmutableSet.of(DESCRIPTOR, DESCRIPTOR_3))
                        .build(7L));
    }

    @Test
    public void leaderChangeWhenGettingUpdateForTransactionsDoesNotThrow() {
        setupInitialState();
        UUID newLeader = UUID.randomUUID();
        LockWatchVersion spuriousVersion = LockWatchVersion.of(newLeader, 99999L);
        TransactionsLockWatchUpdate results =
                eventCache.getUpdateForTransactions(TIMESTAMPS, Optional.of(spuriousVersion));

        assertThat(results.clearCache()).isTrue();
        assertThat(results.startTsToSequence())
                .containsExactlyEntriesOf(ImmutableMap.of(START_TS_1, LockWatchVersion.of(LEADER, 3L)));
        assertThat(results.events())
                .containsExactly(LockWatchCreatedEvent.fromSnapshot((LockWatchStateUpdate.Snapshot) SNAPSHOT));
    }

    @Test
    public void leaderChangeClearsCaches() {
        setupInitialState();
        eventCache.processStartTransactionsUpdate(
                TIMESTAMPS_2, LockWatchStateUpdate.success(EVENT2_UUID, 4L, ImmutableList.of()));
        verifyStage();
    }

    @Test
    public void removingEntriesDoesNotRetentionVersions() {
        setupInitialState();
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_2, SUCCESS);
        verifyStage();

        eventCache.removeTransactionStateFromCache(START_TS_1);
        verifyStage();
    }

    @Test
    public void veryOldSuccessfulUpdateThrowsRetriableException() {
        setupInitialState();
        Set<Long> secondTimestamps = ImmutableSet.of(11L, 12L);

        LockWatchEvent earlyEvent =
                LockEvent.builder(ImmutableSet.of(DESCRIPTOR_3), COMMIT_TOKEN).build(2L);

        assertThatThrownBy(() -> eventCache.processStartTransactionsUpdate(
                        secondTimestamps, LockWatchStateUpdate.success(LEADER, 2L, ImmutableList.of(earlyEvent))))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Cannot process events before the oldest event. The transaction should be retried, "
                        + "although this should only happen very rarely.");
    }

    @Test
    public void nonContiguousEventsThrows() {
        setupInitialState();
        assertThatThrownBy(() -> eventCache.processStartTransactionsUpdate(
                        TIMESTAMPS_2,
                        LockWatchStateUpdate.success(
                                LEADER, 7L, ImmutableList.of(WATCH_EVENT, LOCK_EVENT, LOCK_EVENT_2))))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Events form a non-contiguous sequence");
    }

    @Test
    public void clientOnSameVersionAsCacheDoesNotThrow() {
        setupInitialState();
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_2, SUCCESS);
        assertThat(eventCache
                        .getUpdateForTransactions(
                                TIMESTAMPS_2, Optional.of(LockWatchVersion.of(LEADER, SUCCESS_VERSION)))
                        .events())
                .isEmpty();
    }

    @Test
    public void clientOnSameVersionAsTransactionDoesNotThrow() {
        setupInitialState();
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_2, SUCCESS);
        assertThat(eventCache
                        .getUpdateForTransactions(
                                ImmutableSet.of(START_TS_1, START_TS_2), Optional.of(LockWatchVersion.of(LEADER, 3L)))
                        .events())
                .containsExactly(WATCH_EVENT, UNLOCK_EVENT, LOCK_EVENT);
    }

    @Test
    public void missedEventThrows() {
        setupInitialState();
        assertThatLoggableExceptionThrownBy(() -> eventCache.processStartTransactionsUpdate(
                        TIMESTAMPS_2, LockWatchStateUpdate.success(LEADER, 5L, ImmutableList.of(UNLOCK_EVENT))))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasLogMessage("Events missing between last snapshot and this batch of events");
    }

    @Test
    public void newEventsStartingWithTheSameVersionAsCurrentDoesNotThrow() {
        setupInitialState();
        LockWatchEvent earlyLockEvent = LockWatchCreatedEvent.builder(
                        ImmutableSet.of(REFERENCE), ImmutableSet.of(DESCRIPTOR))
                .build(3L);
        LockWatchStateUpdate success =
                LockWatchStateUpdate.success(LEADER, 4L, ImmutableList.of(earlyLockEvent, WATCH_EVENT));
        assertThatCode(() -> eventCache.processStartTransactionsUpdate(TIMESTAMPS_2, success))
                .doesNotThrowAnyException();
    }

    @Test
    public void timestampEventsRetentionedThrows() {
        createEventCache(1);
        setupInitialState();
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), SUCCESS);

        assertThatThrownBy(() -> eventCache.getUpdateForTransactions(TIMESTAMPS, Optional.empty()))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Events do not enclose the required versions");

        assertThatThrownBy(() -> eventCache.getCommitUpdate(START_TS_1))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("start or commit info not processed for start timestamp");
    }

    @Test
    public void veryFarBehindClientDoesNotCauseEncloseCheckToThrow() {
        LockWatchStateUpdate.Snapshot snapshot =
                LockWatchStateUpdate.snapshot(LEADER, 5000L, ImmutableSet.of(DESCRIPTOR_2), ImmutableSet.of());
        eventCache.processStartTransactionsUpdate(TIMESTAMPS, snapshot);

        TransactionsLockWatchUpdate update =
                eventCache.getUpdateForTransactions(TIMESTAMPS, Optional.of(LockWatchVersion.of(LEADER, 0L)));
        assertThat(update.clearCache()).isTrue();
        assertThat(update.events())
                .containsExactly(LockWatchCreatedEvent.builder(ImmutableSet.of(), ImmutableSet.of(DESCRIPTOR_2))
                        .build(5000L));
        assertThat(update.startTsToSequence())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(START_TS_1, LockWatchVersion.of(LEADER, 5000L)));
    }

    @Test
    public void clientPartiallyUpToDateDoesNotThrow() {
        setupInitialState();
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_2, SUCCESS);

        TransactionsLockWatchUpdate update = eventCache.getUpdateForTransactions(
                Sets.union(TIMESTAMPS, TIMESTAMPS_2), Optional.of(LockWatchVersion.of(LEADER, 5L)));

        assertThat(update.clearCache()).isFalse();
        assertThat(update.events().size()).isEqualTo(1);
        assertThat(update.startTsToSequence())
                .containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                        START_TS_1, LockWatchVersion.of(LEADER, 3L), START_TS_2, LockWatchVersion.of(LEADER, 6L)));
    }

    @Test
    public void newEventsCauseOldEventsToBeDeleted() {
        createEventCache(3);
        setupInitialState();
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), SUCCESS);
        verifyStage();
        eventCache.processStartTransactionsUpdate(TIMESTAMPS_2, SUCCESS_2);
        verifyStage();
    }

    @Test
    public void commitLocksAreCorrectlyFilteredOutUsingServerToken() {
        setupInitialState();
        eventCache.processStartTransactionsUpdate(ImmutableSet.of(), SUCCESS);

        // simulates the actual lock token that the client receives
        ConjureLockToken serverToken = ConjureLockToken.of(COMMIT_TOKEN.getRequestId());
        LockToken commitToken = LeasedLockTokenCreator.of(
                serverToken,
                Lease.of(LeaderTime.of(LeadershipId.random(), NanoTime.createForTests(1L)), Duration.ZERO));
        eventCache.processGetCommitTimestampsUpdate(
                ImmutableSet.of(ImmutableTransactionUpdate.builder()
                        .commitTs(5L)
                        .startTs(START_TS_1)
                        .writesToken(commitToken)
                        .build()),
                SUCCESS_2);

        assertThat(eventCache.getCommitUpdate(START_TS_1).accept(new CommitUpdateVisitor()))
                .containsExactlyInAnyOrder(DESCRIPTOR);
    }

    @Test
    public void equalRangeDoesNotThrow() {
        eventCache.processStartTransactionsUpdate(
                TIMESTAMPS,
                LockWatchStateUpdate.snapshot(LEADER, 10000L, ImmutableSet.of(DESCRIPTOR), ImmutableSet.of()));
        assertThatCode(() -> eventCache.getUpdateForTransactions(
                        TIMESTAMPS, Optional.of(LockWatchVersion.of(LEADER, 10000L))))
                .doesNotThrowAnyException();
    }

    private void setupInitialState() {
        eventCache.processStartTransactionsUpdate(TIMESTAMPS, SNAPSHOT);
    }

    private void createEventCache(int maxSize) {
        fakeCache = NoOpLockWatchEventCache.create();
        realEventCache = new LockWatchEventCacheImpl(LockWatchEventLog.create(maxSize));
        eventCache = new DuplicatingLockWatchEventCache(realEventCache, fakeCache);
    }

    private static final class CommitUpdateVisitor implements CommitUpdate.Visitor<Set<LockDescriptor>> {

        @Override
        public Set<LockDescriptor> invalidateAll() {
            return ImmutableSet.of();
        }

        @Override
        public Set<LockDescriptor> invalidateSome(Set<LockDescriptor> invalidatedLocks) {
            return invalidatedLocks;
        }
    }

    private static final class InvalidatedAllVisitor implements CommitUpdate.Visitor<Boolean> {

        @Override
        public Boolean invalidateAll() {
            return true;
        }

        @Override
        public Boolean invalidateSome(Set<LockDescriptor> invalidatedLocks) {
            return false;
        }
    }
}
