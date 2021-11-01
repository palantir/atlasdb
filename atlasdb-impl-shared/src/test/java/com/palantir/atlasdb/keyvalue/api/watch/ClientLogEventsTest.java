/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.transaction.api.TransactionLockWatchFailedException;
import com.palantir.lock.LockDescriptor;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.client.LeasedLockToken;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.CommitUpdate;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchReferences.LockWatchReference;
import com.palantir.lock.watch.LockWatchVersion;
import com.palantir.lock.watch.TransactionsLockWatchUpdate;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public final class ClientLogEventsTest {
    private static final UUID LEADER = UUID.randomUUID();
    private static final LockToken LOCK_TOKEN_1 = LockToken.of(UUID.randomUUID());
    private static final LockToken LOCK_TOKEN_2 = LockToken.of(UUID.randomUUID());
    private static final ConjureLockToken CONJURE_TOKEN_1 = ConjureLockToken.of(LOCK_TOKEN_1.getRequestId());

    private static final long SEQUENCE_1 = 1;
    private static final long SEQUENCE_2 = 2;
    private static final long SEQUENCE_3 = 3L;
    private static final long SEQUENCE_4 = 4L;
    private static final long TIMESTAMP_1 = 72L;
    private static final long TIMESTAMP_2 = 97L;
    private static final long TIMESTAMP_3 = 99L;
    private static final LockWatchVersion VERSION_0 = LockWatchVersion.of(LEADER, 0L);
    private static final LockWatchVersion VERSION_1 = LockWatchVersion.of(LEADER, SEQUENCE_1);
    private static final LockWatchVersion VERSION_4 = LockWatchVersion.of(LEADER, SEQUENCE_4);

    private static final LockDescriptor DESCRIPTOR_1 = StringLockDescriptor.of("lwelt-one");
    private static final LockDescriptor DESCRIPTOR_2 = StringLockDescriptor.of("lwelt-two");
    private static final LockDescriptor DESCRIPTOR_3 = StringLockDescriptor.of("odd-one-out");

    private static final LockWatchReference REFERENCE_1 = LockWatchReferences.entireTable("table.one");

    private static final LockWatchEvent LOCK_WATCH_EVENT_VERSION_1 = LockWatchCreatedEvent.builder(
                    ImmutableSet.of(REFERENCE_1), ImmutableSet.of(DESCRIPTOR_3))
            .build(SEQUENCE_1);
    private static final LockWatchEvent LOCK_DESCRIPTOR_2_VERSION_2 =
            LockEvent.builder(ImmutableSet.of(DESCRIPTOR_2), LOCK_TOKEN_1).build(SEQUENCE_2);
    private static final LockWatchEvent UNLOCK_DESCRIPTOR_2_VERSION_3 =
            UnlockEvent.builder(ImmutableSet.of(DESCRIPTOR_1)).build(SEQUENCE_3);
    private static final LockWatchEvent LOCK_DESCRIPTOR_1_VERSION_4 =
            LockEvent.builder(ImmutableSet.of(DESCRIPTOR_1), LOCK_TOKEN_2).build(SEQUENCE_4);

    private static final LockWatchEvents EVENTS_2_TO_4 = LockWatchEvents.builder()
            .addEvents(LOCK_DESCRIPTOR_2_VERSION_2, UNLOCK_DESCRIPTOR_2_VERSION_3, LOCK_DESCRIPTOR_1_VERSION_4)
            .build();
    private static final ClientLogEvents CLIENT_EVENTS_2_TO_4_NO_CLEAR_CACHE =
            ClientLogEvents.builder().clearCache(false).events(EVENTS_2_TO_4).build();
    private static final LockWatchEvents EVENTS_3_TO_4 = LockWatchEvents.builder()
            .addEvents(UNLOCK_DESCRIPTOR_2_VERSION_3, LOCK_DESCRIPTOR_1_VERSION_4)
            .build();
    private static final ClientLogEvents CLIENT_EVENTS_3_TO_4_NO_CLEAR_CACHE =
            ClientLogEvents.builder().clearCache(false).events(EVENTS_3_TO_4).build();

    @Test
    public void toTransactionsWithOldClientVersion() {
        TimestampMapping mapping = createTimestampMappingWithSequences(SEQUENCE_2, SEQUENCE_4);
        TransactionsLockWatchUpdate update =
                CLIENT_EVENTS_2_TO_4_NO_CLEAR_CACHE.toTransactionsLockWatchUpdate(mapping, Optional.of(VERSION_1));
        assertThat(update.events())
                .containsExactly(
                        LOCK_DESCRIPTOR_2_VERSION_2, UNLOCK_DESCRIPTOR_2_VERSION_3, LOCK_DESCRIPTOR_1_VERSION_4);
    }

    @Test
    public void toTransactionsWithClientVersionEqualToEarliestTransaction() {
        TimestampMapping mapping = createTimestampMappingWithSequences(SEQUENCE_1, SEQUENCE_4);
        TransactionsLockWatchUpdate update =
                CLIENT_EVENTS_2_TO_4_NO_CLEAR_CACHE.toTransactionsLockWatchUpdate(mapping, Optional.of(VERSION_1));
        assertThat(update.events())
                .containsExactly(
                        LOCK_DESCRIPTOR_2_VERSION_2, UNLOCK_DESCRIPTOR_2_VERSION_3, LOCK_DESCRIPTOR_1_VERSION_4);
    }

    @Test
    public void toTransactionsWithUpToDateClientVersion() {
        LockWatchEvents events = LockWatchEvents.builder().build();
        ClientLogEvents clientLogEvents =
                ClientLogEvents.builder().clearCache(false).events(events).build();

        TimestampMapping mapping = createTimestampMappingWithSequences(SEQUENCE_4, SEQUENCE_4);

        TransactionsLockWatchUpdate update =
                clientLogEvents.toTransactionsLockWatchUpdate(mapping, Optional.of(VERSION_4));
        assertThat(update.events()).isEmpty();
    }

    @Test
    public void toTransactionsWithClientVersionAbsent() {
        TimestampMapping mapping = createTimestampMappingWithSequences(SEQUENCE_2, SEQUENCE_4);
        TransactionsLockWatchUpdate update =
                CLIENT_EVENTS_2_TO_4_NO_CLEAR_CACHE.toTransactionsLockWatchUpdate(mapping, Optional.empty());

        assertThat(update.events())
                .containsExactly(
                        LOCK_DESCRIPTOR_2_VERSION_2, UNLOCK_DESCRIPTOR_2_VERSION_3, LOCK_DESCRIPTOR_1_VERSION_4);
    }

    @Test
    public void toTransactionsWithVeryOldClientVersion() {
        ClientLogEvents clientLogEvents =
                ClientLogEvents.builder().clearCache(true).events(EVENTS_2_TO_4).build();

        TimestampMapping mapping = createTimestampMappingWithSequences(SEQUENCE_2, SEQUENCE_4);
        TransactionsLockWatchUpdate update =
                clientLogEvents.toTransactionsLockWatchUpdate(mapping, Optional.of(VERSION_0));
        assertThat(update.events())
                .containsExactly(
                        LOCK_DESCRIPTOR_2_VERSION_2, UNLOCK_DESCRIPTOR_2_VERSION_3, LOCK_DESCRIPTOR_1_VERSION_4);
    }

    @Test
    public void toTransactionsThrowsIfClientIsBehindEarliestTransactionAndMissingEvents() {
        TimestampMapping mapping = createTimestampMappingWithSequences(SEQUENCE_3, SEQUENCE_4);
        assertThatThrownBy(() -> CLIENT_EVENTS_3_TO_4_NO_CLEAR_CACHE.toTransactionsLockWatchUpdate(
                        mapping, Optional.of(VERSION_1)))
                .as("missing event at sequence 2")
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Events do not enclose the required versions");
    }

    @Test
    public void toTransactionsThrowsIfEventsMissingFromEarliestTimestamp() {
        TimestampMapping mapping = createTimestampMappingWithSequences(SEQUENCE_1, SEQUENCE_4);
        assertThatThrownBy(() -> CLIENT_EVENTS_3_TO_4_NO_CLEAR_CACHE.toTransactionsLockWatchUpdate(
                        mapping, Optional.of(VERSION_1)))
                .as("missing event at sequence 2")
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Events do not enclose the required versions");
    }

    @Test
    public void toTransactionsThrowsIfEventsMissingAndNoClientVersion() {
        TimestampMapping mapping = createTimestampMappingWithSequences(SEQUENCE_1, SEQUENCE_4);
        assertThatThrownBy(() ->
                        CLIENT_EVENTS_3_TO_4_NO_CLEAR_CACHE.toTransactionsLockWatchUpdate(mapping, Optional.empty()))
                .as("missing event at sequence 2")
                .isExactlyInstanceOf(TransactionLockWatchFailedException.class)
                .hasMessage("Events do not enclose the required versions");
    }

    @Test
    public void toCommitUpdateInvalidatesAllWithClearCacheTrue() {
        TimestampStateStore.CommitInfo commitInfo = TimestampStateStore.CommitInfo.of(LOCK_TOKEN_1, VERSION_1);

        ClientLogEvents clientEventsWithClearCache = ClientLogEvents.builder()
                .from(CLIENT_EVENTS_2_TO_4_NO_CLEAR_CACHE)
                .clearCache(true)
                .build();
        assertThat(isInvalidateAll(clientEventsWithClearCache.toCommitUpdate(VERSION_1, VERSION_1, Optional.empty())))
                .as("toCommitUpdate should ignore provided commit info when clear cache is true")
                .isTrue();
        assertThat(isInvalidateAll(
                        clientEventsWithClearCache.toCommitUpdate(VERSION_1, VERSION_1, Optional.of(commitInfo))))
                .as("toCommitUpdate should ignore provided commit info when clear cache is true")
                .isTrue();
    }

    @Test
    public void toCommitUpdateFiltersOutUnlockEvents() {
        LockWatchVersion startVersion = LockWatchVersion.of(LEADER, SEQUENCE_1);
        LockWatchVersion endVersion = LockWatchVersion.of(LEADER, SEQUENCE_4);
        CommitUpdate commitUpdate =
                CLIENT_EVENTS_2_TO_4_NO_CLEAR_CACHE.toCommitUpdate(startVersion, endVersion, Optional.empty());
        Set<LockDescriptor> lockDescriptors = extractLockDescriptors(commitUpdate);
        assertThat(lockDescriptors).containsExactlyInAnyOrder(DESCRIPTOR_1, DESCRIPTOR_2);
    }

    @Test
    public void toCommitUpdateFiltersOutLockEventsWithMatchingLockToken() {
        // due to how the commit flow works, the filtering is only done if it is a leased lock token
        LeasedLockToken leasedLockToken = mock(LeasedLockToken.class);
        when(leasedLockToken.serverToken()).thenReturn(CONJURE_TOKEN_1);
        TimestampStateStore.CommitInfo commitInfo = TimestampStateStore.CommitInfo.of(leasedLockToken, VERSION_1);

        CommitUpdate commitUpdate =
                CLIENT_EVENTS_2_TO_4_NO_CLEAR_CACHE.toCommitUpdate(VERSION_1, VERSION_4, Optional.of(commitInfo));
        Set<LockDescriptor> lockDescriptors = extractLockDescriptors(commitUpdate);
        assertThat(lockDescriptors).containsExactlyInAnyOrder(DESCRIPTOR_1);
    }

    @Test
    public void toCommitUpdateDoesNotFilterOutBasedOnNonLeasedLockTokens() {
        // due to how the commit flow works, the filtering is only done if it is a leased lock token
        TimestampStateStore.CommitInfo commitInfo = TimestampStateStore.CommitInfo.of(LOCK_TOKEN_1, VERSION_1);

        CommitUpdate commitUpdate =
                CLIENT_EVENTS_2_TO_4_NO_CLEAR_CACHE.toCommitUpdate(VERSION_1, VERSION_4, Optional.of(commitInfo));
        Set<LockDescriptor> lockDescriptors = extractLockDescriptors(commitUpdate);
        assertThat(lockDescriptors).containsExactlyInAnyOrder(DESCRIPTOR_1, DESCRIPTOR_2);
    }

    @Test
    public void toCommitUpdateAlsoIncludesDescriptorsFromWatchEvents() {
        LockWatchEvents events = LockWatchEvents.builder()
                .addEvents(
                        LOCK_WATCH_EVENT_VERSION_1,
                        LOCK_DESCRIPTOR_2_VERSION_2,
                        UNLOCK_DESCRIPTOR_2_VERSION_3,
                        LOCK_DESCRIPTOR_1_VERSION_4)
                .build();
        ClientLogEvents clientLogEvents =
                ClientLogEvents.builder().clearCache(false).events(events).build();

        CommitUpdate commitUpdate = clientLogEvents.toCommitUpdate(VERSION_0, VERSION_4, Optional.empty());
        Set<LockDescriptor> lockDescriptors = extractLockDescriptors(commitUpdate);
        assertThat(lockDescriptors).containsExactlyInAnyOrder(DESCRIPTOR_1, DESCRIPTOR_2, DESCRIPTOR_3);
    }

    private static Set<LockDescriptor> extractLockDescriptors(CommitUpdate commitUpdate) {
        return commitUpdate.accept(new CommitUpdate.Visitor<>() {
            @Override
            public Set<LockDescriptor> invalidateAll() {
                throw new SafeIllegalStateException("commit update was invalidate all");
            }

            @Override
            public Set<LockDescriptor> invalidateSome(Set<LockDescriptor> invalidatedLocks) {
                return invalidatedLocks;
            }
        });
    }

    private static boolean isInvalidateAll(CommitUpdate commitUpdate) {
        return commitUpdate.accept(new CommitUpdate.Visitor<>() {
            @Override
            public Boolean invalidateAll() {
                return true;
            }

            @Override
            public Boolean invalidateSome(Set<LockDescriptor> _invalidatedLocks) {
                return false;
            }
        });
    }

    private static TimestampMapping createTimestampMappingWithSequences(long lowerSequence, long upperSequence) {
        // Create a timestamp mapping with timestamps at the extremes, as well as one in the middle to confirm that it
        // does not influence the checks here
        return TimestampMapping.builder()
                .putTimestampMapping(TIMESTAMP_1, LockWatchVersion.of(LEADER, lowerSequence))
                .putTimestampMapping(TIMESTAMP_2, LockWatchVersion.of(LEADER, (upperSequence + lowerSequence) / 2))
                .putTimestampMapping(TIMESTAMP_3, LockWatchVersion.of(LEADER, upperSequence))
                .build();
    }
}
