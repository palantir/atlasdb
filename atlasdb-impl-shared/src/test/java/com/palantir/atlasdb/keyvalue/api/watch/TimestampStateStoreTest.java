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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public final class TimestampStateStoreTest {
    private static final UUID LEADER = UUID.randomUUID();
    private static final LockWatchVersion VERSION_1 = LockWatchVersion.of(LEADER, 1L);
    private static final LockWatchVersion VERSION_2 = LockWatchVersion.of(LEADER, 17L);
    private static final LockToken WRITES_TOKEN = LockToken.of(UUID.randomUUID());
    private static final LockToken WRITES_TOKEN_2 = LockToken.of(UUID.randomUUID());
    private static final TransactionUpdate UPDATE = TransactionUpdate.builder()
            .startTs(100L)
            .commitTs(400L)
            .writesToken(WRITES_TOKEN)
            .build();
    private static final TransactionUpdate UPDATE_2 = TransactionUpdate.builder()
            .startTs(102L)
            .commitTs(402L)
            .writesToken(WRITES_TOKEN_2)
            .build();

    private TimestampStateStore timestampStateStore;

    @Before
    public void before() {
        timestampStateStore = new TimestampStateStore();
    }

    @Test
    public void cannotPutStartTimestampTwice() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(100L, 102L, 104L), VERSION_1);
        assertThat(timestampStateStore.getStartVersion(102L)).hasValue(VERSION_1);

        assertThatThrownBy(() -> timestampStateStore.putStartTimestamps(ImmutableSet.of(101L, 103L, 104L), VERSION_2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Start timestamp already present in map");
        timestampStateStore.getStartVersion(101L).ifPresent(value -> assertThat(value)
                .isEqualTo(VERSION_2));
    }

    @Test
    public void commitUpdateDoesNotChangeStartVersion() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(100L), VERSION_1);
        timestampStateStore.putCommitUpdates(ImmutableSet.of(UPDATE), VERSION_2);

        assertThat(timestampStateStore.getStartVersion(100L)).hasValue(VERSION_1);
        assertThat(timestampStateStore.getCommitInfo(100L))
                .hasValue(TimestampStateStore.CommitInfo.of(WRITES_TOKEN, VERSION_2));
    }

    @Test
    public void cannotPutCommitUpdateBeforeStartUpdate() {
        assertThatThrownBy(() -> timestampStateStore.putCommitUpdates(ImmutableSet.of(UPDATE), VERSION_2))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("start timestamp missing from map");
    }

    @Test
    public void cannotPutCommitUpdateTwice() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(100L), VERSION_1);
        timestampStateStore.putCommitUpdates(ImmutableSet.of(UPDATE), VERSION_2);
        TransactionUpdate badUpdate = TransactionUpdate.builder()
                .startTs(100L)
                .commitTs(402L)
                .writesToken(WRITES_TOKEN_2)
                .build();

        assertThatThrownBy(() -> timestampStateStore.putCommitUpdates(ImmutableSet.of(UPDATE), VERSION_1))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Commit info already present for given timestamp");
        assertThat(timestampStateStore.getCommitInfo(100L))
                .hasValue(TimestampStateStore.CommitInfo.of(WRITES_TOKEN, VERSION_2));

        assertThatThrownBy(() -> timestampStateStore.putCommitUpdates(ImmutableSet.of(badUpdate), VERSION_2))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Commit info already present for given timestamp");
        assertThat(timestampStateStore.getCommitInfo(100L))
                .hasValue(TimestampStateStore.CommitInfo.of(WRITES_TOKEN, VERSION_2));
    }

    @Test
    public void canPutMultipleUpdatesForDifferentStartTimestamps() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(100L, 102L, 104L), VERSION_1);
        timestampStateStore.putCommitUpdates(ImmutableSet.of(UPDATE, UPDATE_2), VERSION_2);

        assertThat(timestampStateStore.getCommitInfo(100L))
                .hasValue(TimestampStateStore.CommitInfo.of(WRITES_TOKEN, VERSION_2));
        assertThat(timestampStateStore.getCommitInfo(102L))
                .hasValue(TimestampStateStore.CommitInfo.of(WRITES_TOKEN_2, VERSION_2));
    }

    @Test
    public void canRemoveVersions() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(100L, 102L), VERSION_1);
        timestampStateStore.putCommitUpdates(ImmutableSet.of(UPDATE), VERSION_2);

        timestampStateStore.remove(100L);
        assertThat(timestampStateStore.getStartVersion(100L)).isEmpty();
        assertThat(timestampStateStore.getCommitInfo(100L)).isEmpty();
        assertThat(timestampStateStore.getStartVersion(102L)).hasValue(VERSION_1);

        timestampStateStore.remove(102L);
        assertThat(timestampStateStore.getStartVersion(102L)).isEmpty();
    }

    @Test
    public void canClear() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(100L, 102L), VERSION_1);
        timestampStateStore.putCommitUpdates(ImmutableSet.of(UPDATE), VERSION_2);

        timestampStateStore.clear();
        assertThat(timestampStateStore.getStartVersion(100L)).isEmpty();
        assertThat(timestampStateStore.getCommitInfo(100L)).isEmpty();
        assertThat(timestampStateStore.getStartVersion(102L)).isEmpty();
        assertThat(timestampStateStore.getEarliestLiveSequence()).isEmpty();
    }

    @Test
    public void earliestVersionUpdatesWhenAllTimestampsRemovedForVersion() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(100L, 200L), VERSION_1);
        timestampStateStore.putStartTimestamps(ImmutableSet.of(400L, 800L), VERSION_2);

        assertThat(timestampStateStore.getEarliestLiveSequence()).hasValue(Sequence.of(1L));

        removeAndCheckEarliestVersion(100L, 1L);
        removeAndCheckEarliestVersion(800L, 1L);
        removeAndCheckEarliestVersion(200L, 17L);

        timestampStateStore.remove(400L);
        assertThat(timestampStateStore.getEarliestLiveSequence()).isEmpty();
    }

    @Test
    public void earliestVersionUpdatesWhenLowerTimestampsAreAdded() {
        timestampStateStore.putStartTimestamps(ImmutableSet.of(400L, 800L), VERSION_2);
        assertThat(timestampStateStore.getEarliestLiveSequence()).hasValue(Sequence.of(17L));

        timestampStateStore.putStartTimestamps(ImmutableSet.of(100L, 200L), VERSION_1);
        assertThat(timestampStateStore.getEarliestLiveSequence()).hasValue(Sequence.of(1L));
    }

    private void removeAndCheckEarliestVersion(long timestamp, long sequence) {
        timestampStateStore.remove(timestamp);
        assertThat(timestampStateStore.getEarliestLiveSequence()).hasValue(Sequence.of(sequence));
    }
}
