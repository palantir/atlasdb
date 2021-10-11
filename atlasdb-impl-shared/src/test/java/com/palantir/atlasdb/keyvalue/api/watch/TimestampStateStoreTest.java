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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.watch.TimestampStateStore.CommitInfo;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.junit.Before;
import org.junit.Test;

public final class TimestampStateStoreTest {
    private static final UUID LEADER = UUID.randomUUID();
    private static final LockWatchVersion VERSION_1 = LockWatchVersion.of(LEADER, 1L);
    private static final LockWatchVersion VERSION_2 = LockWatchVersion.of(LEADER, 17L);
    private static final LockWatchVersion VERSION_3 = LockWatchVersion.of(LEADER, 88L);
    private static final long TIMESTAMP_1 = 100L;
    private static final long TIMESTAMP_2 = 200L;
    private static final long TIMESTAMP_3 = 400L;
    private static final long TIMESTAMP_4 = 800L;

    private TimestampStateStore timestampStateStore;

    @Before
    public void before() {
        timestampStateStore = new TimestampStateStore();
    }

    @Test
    public void cannotPutSameStartTimestampTwice() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1), VERSION_1);
        assertThatThrownBy(() -> timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1), VERSION_1))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Start timestamp already present in map");
        assertThatThrownBy(() -> timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1), VERSION_2))
                .isInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Start timestamp already present in map");
    }

    @Test
    public void emptyStoreHasNoEarliestSequence() {
        assertThat(timestampStateStore.getEarliestLiveSequence()).isEmpty();
    }

    @Test
    public void getEarliestLiveSequenceFindsEarliestVersion() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), VERSION_1);
        assertEarliestVersion(VERSION_1);
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_3, TIMESTAMP_4), VERSION_2);
        assertEarliestVersion(VERSION_1);
    }

    @Test
    public void earliestLiveSequenceMovesBackwardsOnEarlierAddition() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_3, TIMESTAMP_4), VERSION_2);
        assertEarliestVersion(VERSION_2);
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), VERSION_1);
        assertEarliestVersion(VERSION_1);
    }

    @Test
    public void startTimestampAndCommitUpdateNoLongerReadableAfterRemoval() {
        LockToken token = LockToken.of(UUID.randomUUID());
        TransactionUpdate update = TransactionUpdate.builder()
                .startTs(TIMESTAMP_1)
                .commitTs(TIMESTAMP_2)
                .writesToken(token)
                .build();

        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1), VERSION_1);
        timestampStateStore.putCommitUpdates(ImmutableSet.of(update), VERSION_2);

        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_1)).hasValue(VERSION_1);
        assertThat(timestampStateStore.getCommitInfo(TIMESTAMP_1)).hasValue(CommitInfo.of(token, VERSION_2));

        timestampStateStore.remove(TIMESTAMP_1);

        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_1)).isEmpty();
        assertThat(timestampStateStore.getCommitInfo(TIMESTAMP_1)).isEmpty();
    }

    @Test
    public void earliestLiveSequenceMovesForwardWithSequentialRemovals() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), VERSION_1);
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_3, TIMESTAMP_4), VERSION_2);

        removeAndAssertEarliestVersion(TIMESTAMP_1, VERSION_1);
        removeAndAssertEarliestVersion(TIMESTAMP_2, VERSION_2);
        removeAndAssertEarliestVersion(TIMESTAMP_3, VERSION_2);
        removeAndAssertNoEarliestVersion(TIMESTAMP_4);
    }

    @Test
    public void earliestVersionUpdatesOnlyWhenAllTimestampsRemovedForVersion() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), VERSION_1);
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_3, TIMESTAMP_4), VERSION_2);
        assertEarliestVersion(VERSION_1);

        removeAndAssertEarliestVersion(TIMESTAMP_1, VERSION_1);
        removeAndAssertEarliestVersion(TIMESTAMP_3, VERSION_1);
        removeAndAssertEarliestVersion(TIMESTAMP_2, VERSION_2);
        removeAndAssertNoEarliestVersion(TIMESTAMP_4);
    }

    @Test
    public void removalOfUnknownTimestampIsNoOp() {
        assertThatCode(() -> timestampStateStore.remove(TIMESTAMP_1)).doesNotThrowAnyException();
    }

    @Test
    public void clearRemovesAllMappings() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), VERSION_1);
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_3, TIMESTAMP_4), VERSION_2);
        assertEarliestVersion(VERSION_1);

        timestampStateStore.clear();
        assertThat(timestampStateStore.getEarliestLiveSequence()).isEmpty();
        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_1)).isEmpty();
        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_3)).isEmpty();
    }

    @Test
    public void getStartVersionWithoutCommitInfo() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1), VERSION_1);
        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_1)).hasValue(VERSION_1);
    }

    @Test
    public void returnsEmptyWhenQueryingStartVersionOfUnknownTimestamp() {
        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_1)).isEmpty();
    }

    @Test
    public void canPutCommitUpdate() {
        LockToken token = LockToken.of(UUID.randomUUID());
        TransactionUpdate update = TransactionUpdate.builder()
                .startTs(TIMESTAMP_1)
                .commitTs(TIMESTAMP_2)
                .writesToken(token)
                .build();

        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1), VERSION_1);
        timestampStateStore.putCommitUpdates(ImmutableSet.of(update), VERSION_2);

        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_1)).hasValue(VERSION_1);
        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_2)).isEmpty();
        assertThat(timestampStateStore.getCommitInfo(TIMESTAMP_1)).hasValue(CommitInfo.of(token, VERSION_2));
        assertThat(timestampStateStore.getCommitInfo(TIMESTAMP_2)).isEmpty();
    }

    @Test
    public void handlesBatchedCommitUpdateForSeparateTransactionStarts() {
        LockToken token1 = LockToken.of(UUID.randomUUID());
        LockToken token2 = LockToken.of(UUID.randomUUID());

        TransactionUpdate update1 = TransactionUpdate.builder()
                .startTs(TIMESTAMP_1)
                .commitTs(TIMESTAMP_3)
                .writesToken(token1)
                .build();
        TransactionUpdate update2 = TransactionUpdate.builder()
                .startTs(TIMESTAMP_2)
                .commitTs(TIMESTAMP_4)
                .writesToken(token2)
                .build();

        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1), VERSION_1);
        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_2), VERSION_2);
        timestampStateStore.putCommitUpdates(ImmutableSet.of(update1, update2), VERSION_3);

        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_1)).hasValue(VERSION_1);
        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_2)).hasValue(VERSION_2);
        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_3)).isEmpty();
        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_4)).isEmpty();
        assertThat(timestampStateStore.getCommitInfo(TIMESTAMP_1)).hasValue(CommitInfo.of(token1, VERSION_3));
        assertThat(timestampStateStore.getCommitInfo(TIMESTAMP_2)).hasValue(CommitInfo.of(token2, VERSION_3));
        assertThat(timestampStateStore.getCommitInfo(TIMESTAMP_3)).isEmpty();
        assertThat(timestampStateStore.getCommitInfo(TIMESTAMP_4)).isEmpty();
    }

    @Test
    public void handlesSeparateCommitUpdatesForBatchedTransactionStart() {
        LockToken token1 = LockToken.of(UUID.randomUUID());
        LockToken token2 = LockToken.of(UUID.randomUUID());

        TransactionUpdate update1 = TransactionUpdate.builder()
                .startTs(TIMESTAMP_1)
                .commitTs(TIMESTAMP_3)
                .writesToken(token1)
                .build();
        TransactionUpdate update2 = TransactionUpdate.builder()
                .startTs(TIMESTAMP_2)
                .commitTs(TIMESTAMP_4)
                .writesToken(token2)
                .build();

        timestampStateStore.putStartTimestamps(ImmutableSet.of(TIMESTAMP_1, TIMESTAMP_2), VERSION_1);
        timestampStateStore.putCommitUpdates(ImmutableSet.of(update1), VERSION_2);
        timestampStateStore.putCommitUpdates(ImmutableSet.of(update2), VERSION_3);

        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_1)).hasValue(VERSION_1);
        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_2)).hasValue(VERSION_1);
        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_3)).isEmpty();
        assertThat(timestampStateStore.getStartVersion(TIMESTAMP_4)).isEmpty();
        assertThat(timestampStateStore.getCommitInfo(TIMESTAMP_1)).hasValue(CommitInfo.of(token1, VERSION_2));
        assertThat(timestampStateStore.getCommitInfo(TIMESTAMP_2)).hasValue(CommitInfo.of(token2, VERSION_3));
        assertThat(timestampStateStore.getCommitInfo(TIMESTAMP_3)).isEmpty();
        assertThat(timestampStateStore.getCommitInfo(TIMESTAMP_4)).isEmpty();
    }

    @Test
    public void cannotPutCommitUpdateTwice() {
        TransactionUpdate update = TransactionUpdate.builder()
                .startTs(TIMESTAMP_1)
                .commitTs(TIMESTAMP_2)
                .writesToken(LockToken.of(UUID.randomUUID()))
                .build();

        timestampStateStore.putStartTimestamps(ImmutableSet.of(100L), VERSION_1);
        timestampStateStore.putCommitUpdates(ImmutableSet.of(update), VERSION_2);
        assertThatThrownBy(() -> timestampStateStore.putCommitUpdates(ImmutableSet.of(update), VERSION_2))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Commit info already present for given timestamp");
    }

    @Test
    public void cannotPutCommitUpdateBeforeStartUpdate() {
        TransactionUpdate update = TransactionUpdate.builder()
                .startTs(TIMESTAMP_1)
                .commitTs(TIMESTAMP_2)
                .writesToken(LockToken.of(UUID.randomUUID()))
                .build();

        assertThatThrownBy(() -> timestampStateStore.putCommitUpdates(ImmutableSet.of(update), VERSION_2))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Start timestamp missing from map");
    }

    @Test
    public void preventsAdditionsWhenTimestampStateStoreSizeExceeded() {
        Set<Long> overlyLargeTimestampSet = LongStream.range(0, TimestampStateStore.MAXIMUM_SIZE + 1)
                .boxed()
                .collect(Collectors.toSet());
        timestampStateStore.putStartTimestamps(overlyLargeTimestampSet, VERSION_1);
        assertThatThrownBy(() -> timestampStateStore.putStartTimestamps(
                        ImmutableSet.of(TimestampStateStore.MAXIMUM_SIZE + 1L), VERSION_2))
                .as("Cannot add a timestamp when the store has exceeded its maximum size")
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessage("Exceeded maximum timestamp state store size");
    }

    private void removeAndAssertEarliestVersion(long timestamp, LockWatchVersion version) {
        timestampStateStore.remove(timestamp);
        assertEarliestVersion(version);
    }

    private void removeAndAssertNoEarliestVersion(long timestamp) {
        timestampStateStore.remove(timestamp);
        assertThat(timestampStateStore.getEarliestLiveSequence()).isEmpty();
    }

    private void assertEarliestVersion(LockWatchVersion version) {
        assertThat(timestampStateStore.getEarliestLiveSequence()).hasValue(Sequence.of(version.version()));
    }
}
