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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.ImmutableTransactionUpdate;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionUpdate;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public final class TimestampStateStoreTest {
    private static UUID leader = UUID.randomUUID();
    private static LockWatchVersion version1 = LockWatchVersion.of(leader, 1L);
    private static LockWatchVersion version2 = LockWatchVersion.of(leader, 17L);

    private TimestampStateStore timestampStateStore;

    @Before
    public void before() {
        timestampStateStore = new TimestampStateStore();
    }

    @Test
    public void cannotPutCommitUpdateTwice() {
        TransactionUpdate update = ImmutableTransactionUpdate.builder()
                .startTs(100L)
                .commitTs(400L)
                .writesToken(LockToken.of(UUID.randomUUID()))
                .build();

        timestampStateStore.putStartTimestamps(ImmutableSet.of(100L), version1);
        timestampStateStore.putCommitUpdates(ImmutableSet.of(update), version2);
        assertThatThrownBy(() -> timestampStateStore.putCommitUpdates(ImmutableSet.of(update), version2))
                .isExactlyInstanceOf(SafeIllegalArgumentException.class)
                .hasMessage("Commit info already present for given timestamp");
    }

    @Test
    public void cannotPutCommitUpdateBeforeStartUpdate() {
        TransactionUpdate update = ImmutableTransactionUpdate.builder()
                .startTs(100L)
                .commitTs(400L)
                .writesToken(LockToken.of(UUID.randomUUID()))
                .build();

        assertThatThrownBy(() -> timestampStateStore.putCommitUpdates(ImmutableSet.of(update), version2))
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("start timestamp missing from map");
    }
}
