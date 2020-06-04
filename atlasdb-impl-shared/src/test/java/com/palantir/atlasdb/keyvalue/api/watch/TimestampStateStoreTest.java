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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.IdentifiedVersion;
import com.palantir.lock.watch.ImmutableTransactionUpdate;
import com.palantir.lock.watch.TransactionUpdate;

public final class TimestampStateStoreTest {
    private static UUID LEADER = UUID.randomUUID();

    private static IdentifiedVersion VERSION_1 = IdentifiedVersion.of(LEADER, 1L);
    private static IdentifiedVersion VERSION_2 = IdentifiedVersion.of(LEADER, 17L);

    private TimestampStateStore timestampStateStore;

    @Before
    public void before() {
        timestampStateStore = new TimestampStateStore();
    }

    @Test
    public void earliestVersionUpdatesWhenAllTimestampsRemovedForVersion() {
        timestampStateStore.putStartTimestamps(100L, VERSION_1);
        timestampStateStore.putStartTimestamps(200L, VERSION_1);
        timestampStateStore.putStartTimestamps(400L, VERSION_2);
        timestampStateStore.putStartTimestamps(800L, VERSION_2);

        assertThat(timestampStateStore.getEarliestVersion()).hasValue(1L);

        removeAndCheckEarliestVersion(100L, 1L);
        removeAndCheckEarliestVersion(800L, 1L);
        removeAndCheckEarliestVersion(200L, 17L);

        timestampStateStore.remove(400L);
        assertThat(timestampStateStore.getEarliestVersion()).isEmpty();
    }

    @Test
    public void cannotPutCommitUpdateTwice() {
        TransactionUpdate update = ImmutableTransactionUpdate.builder()
                .startTs(100L)
                .commitTs(400L)
                .writesToken(LockToken.of(UUID.randomUUID()))
                .build();

        timestampStateStore.putStartTimestamps(100L, VERSION_1);
        assertThat(timestampStateStore.putCommitUpdates(update, VERSION_2)).isTrue();
        assertThat(timestampStateStore.putCommitUpdates(update, VERSION_2)).isFalse();
    }

    @Test
    public void cannotPutCommitUpdateBeforeStartUpdate() {
        TransactionUpdate update = ImmutableTransactionUpdate.builder()
                .startTs(100L)
                .commitTs(400L)
                .writesToken(LockToken.of(UUID.randomUUID()))
                .build();

        assertThat(timestampStateStore.putCommitUpdates(update, VERSION_2)).isFalse();
    }

    private void removeAndCheckEarliestVersion(long timestamp, long sequence) {
        timestampStateStore.remove(timestamp);
        assertThat(timestampStateStore.getEarliestVersion()).hasValue(sequence);
    }
}
