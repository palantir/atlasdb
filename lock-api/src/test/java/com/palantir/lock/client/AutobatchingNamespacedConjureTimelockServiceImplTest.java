/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.lock.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Longs;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureIdentifiedVersion;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.TimeLockCommandOutput;
import com.palantir.atlasdb.timelock.api.TimeLockCommands;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.lock.StringLockDescriptor;
import com.palantir.lock.generated.Command.CommandOutput;
import com.palantir.lock.generated.Command.CommandSet;
import com.palantir.lock.generated.Command.LeaderTime;
import com.palantir.lock.generated.Command.Lease;
import com.palantir.lock.generated.Command.LockImmutableTimestampResponse;
import com.palantir.lock.generated.Command.LockWatchVersion;
import com.palantir.lock.generated.Command.PartitionedTimestamps;
import com.palantir.lock.generated.Command.StartTransactionsResponse;
import com.palantir.lock.generated.Command.TimestampRange;
import com.palantir.lock.generated.Command.TransactionStartRequest;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.watch.LockEvent;
import com.palantir.lock.watch.LockWatchCreatedEvent;
import com.palantir.lock.watch.LockWatchEvent;
import com.palantir.lock.watch.LockWatchReferences;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchStateUpdate.Snapshot;
import com.palantir.lock.watch.LockWatchStateUpdate.Success;
import com.palantir.lock.watch.LockWatchStateUpdate.Visitor;
import com.palantir.lock.watch.UnlockEvent;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Optional;
import java.util.UUID;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class AutobatchingNamespacedConjureTimelockServiceImplTest {
    private final NamespacedConjureTimelockService namespacedConjureTimelockService =
            mock(NamespacedConjureTimelockService.class);
    private final AutobatchingNamespacedConjureTimelockServiceImpl autobatchingNamespacedConjureTimelockService =
            new AutobatchingNamespacedConjureTimelockServiceImpl(namespacedConjureTimelockService);

    @Test
    public void canGetTimestamps() throws InvalidProtocolBufferException {
        when(namespacedConjureTimelockService.runCommands(any()))
                .thenReturn(TimeLockCommandOutput.of(Bytes.from(CommandOutput.newBuilder()
                        .setTimestamps(TimestampRange.newBuilder()
                                .setStartInclusive(1)
                                .setNumGiven(10)
                                .build())
                        .build()
                        .toByteArray())));
        ConjureGetFreshTimestampsResponse response = autobatchingNamespacedConjureTimelockService.getFreshTimestamps(
                ConjureGetFreshTimestampsRequest.of(10));
        assertThat(response.getInclusiveLower()).isEqualTo(1L);
        assertThat(response.getInclusiveUpper()).isEqualTo(10L);
        ArgumentCaptor<TimeLockCommands> captor = ArgumentCaptor.forClass(TimeLockCommands.class);
        verify(namespacedConjureTimelockService).runCommands(captor.capture());
        TimeLockCommands commands = captor.getValue();
        assertThat(CommandSet.parseFrom(commands.get().asNewByteArray()))
                .satisfies(cs -> assertThat(cs.getTimestampsToRetrieve()).isEqualTo(10));
    }

    @Test
    public void canStartTransactionsWithoutLockWatches()
            throws InvalidProtocolBufferException, JsonProcessingException {
        UUID requestId = UUID.randomUUID();
        UUID requestorId = UUID.randomUUID();
        UUID lockToken = UUID.randomUUID();
        UUID leaderId = UUID.randomUUID();

        ObjectMapper objectMapper = ObjectMappers.newSmileServerObjectMapper();
        Snapshot snapshot = LockWatchStateUpdate.snapshot(
                leaderId,
                57,
                ImmutableSet.of(StringLockDescriptor.of("tomato")),
                ImmutableSet.of(LockWatchReferences.entireTable("tom")));
        byte[] lockWatchState = objectMapper.writeValueAsBytes(
                ImmutableStateUpdatePair.builder().snapshot(snapshot).build());

        when(namespacedConjureTimelockService.runCommands(any()))
                .thenReturn(TimeLockCommandOutput.of(Bytes.from(CommandOutput.newBuilder()
                        .addStartedTransactions(StartTransactionsResponse.newBuilder()
                                .setRequestId(ByteString.copyFrom(toBytes(requestId)))
                                .setPartitionedTimestamps(PartitionedTimestamps.newBuilder()
                                        .setStart(20)
                                        .setCount(10)
                                        .setInterval(16)
                                        .build())
                                .setLockImmutableTimestampResponse(LockImmutableTimestampResponse.newBuilder()
                                        .setTokenId(ByteString.copyFrom(toBytes(lockToken)))
                                        .setTimestamp(19)
                                        .build())
                                .setLease(Lease.newBuilder()
                                        .setLeaderTime(LeaderTime.newBuilder()
                                                .setLeaderId(ByteString.copyFrom(toBytes(leaderId)))
                                                .setTime(888888888)
                                                .build())
                                        .setValidityNanos(123456789)
                                        .build())
                                .build())
                        .setValueAndMultipleStateUpdates(ByteString.copyFrom(lockWatchState))
                        .build()
                        .toByteArray())));
        ConjureStartTransactionsResponse response =
                autobatchingNamespacedConjureTimelockService.startTransactions(ConjureStartTransactionsRequest.builder()
                        .requestId(requestId)
                        .requestorId(requestorId)
                        .numTransactions(10)
                        .lastKnownVersion(Optional.empty())
                        .build());
        assertThat(response.getTimestamps()).satisfies(partitionedTimestamps -> {
            assertThat(partitionedTimestamps.start()).isEqualTo(20L);
            assertThat(partitionedTimestamps.count()).isEqualTo(10);
            assertThat(partitionedTimestamps.interval()).isEqualTo(16);
        });
        assertThat(response.getImmutableTimestamp()).satisfies(lockImmutableTimestampResponse -> {
            assertThat(lockImmutableTimestampResponse.getLock().getRequestId()).isEqualTo(lockToken);
            assertThat(lockImmutableTimestampResponse.getImmutableTimestamp()).isEqualTo(19);
        });
        assertThat(response.getLease()).satisfies(lease -> {
            assertThat(lease.leaderTime().id().id()).isEqualTo(leaderId);
            assertThat(lease.leaderTime().currentTime().time()).isEqualTo(888888888);
            assertThat(lease.validity().toNanos()).isEqualTo(123456789);
        });
        assertThat(response.getLockWatchUpdate()).isEqualTo(snapshot);

        ArgumentCaptor<TimeLockCommands> captor = ArgumentCaptor.forClass(TimeLockCommands.class);
        verify(namespacedConjureTimelockService).runCommands(captor.capture());
        TimeLockCommands commands = captor.getValue();
        assertThat(CommandSet.parseFrom(commands.get().asNewByteArray())).satisfies(cs -> {
            TransactionStartRequest onlyRequest = Iterables.getOnlyElement(cs.getTransactionStartRequestsList());
            assertThat(toUuid(onlyRequest.getRequestId().toByteArray())).isEqualTo(requestId);
            assertThat(toUuid(onlyRequest.getRequestorId().toByteArray())).isEqualTo(requestorId);
            assertThat(onlyRequest.getTransactionsToStart()).isEqualTo(10);
            assertThat(cs.getNeedGenericLockWatchUpdate()).isTrue();
        });
    }

    @Test
    public void canStartTransactionsWithLockWatches() throws JsonProcessingException, InvalidProtocolBufferException {
        UUID requestId = UUID.randomUUID();
        UUID requestorId = UUID.randomUUID();
        UUID lockToken = UUID.randomUUID();
        UUID anotherLockToken = UUID.randomUUID();
        UUID leaderId = UUID.randomUUID();

        ObjectMapper objectMapper = ObjectMappers.newSmileServerObjectMapper();
        Snapshot snapshot = LockWatchStateUpdate.snapshot(
                leaderId,
                57,
                ImmutableSet.of(StringLockDescriptor.of("tomato")),
                ImmutableSet.of(LockWatchReferences.entireTable("tom")));
        LockWatchEvent lockEvent = LockEvent.builder(
                        ImmutableSet.of(StringLockDescriptor.of("tomorrow")), LockToken.of(anotherLockToken))
                .build(55);
        LockWatchEvent unlockEvent = UnlockEvent.builder(ImmutableSet.of(StringLockDescriptor.of("tomorrow")))
                .build(56);
        LockWatchEvent createEvent = LockWatchCreatedEvent.builder(
                        ImmutableSet.of(LockWatchReferences.entireTable("tom")), ImmutableSet.of())
                .build(57);
        Success success =
                LockWatchStateUpdate.success(leaderId, 54, ImmutableList.of(lockEvent, unlockEvent, createEvent));
        byte[] lockWatchState = objectMapper.writeValueAsBytes(ImmutableStateUpdatePair.builder()
                .snapshot(snapshot)
                .oldestSuccess(success)
                .build());

        when(namespacedConjureTimelockService.runCommands(any()))
                .thenReturn(TimeLockCommandOutput.of(Bytes.from(CommandOutput.newBuilder()
                        .addStartedTransactions(StartTransactionsResponse.newBuilder()
                                .setRequestId(ByteString.copyFrom(toBytes(requestId)))
                                .setPartitionedTimestamps(PartitionedTimestamps.newBuilder()
                                        .setStart(20)
                                        .setCount(10)
                                        .setInterval(16)
                                        .build())
                                .setLockImmutableTimestampResponse(LockImmutableTimestampResponse.newBuilder()
                                        .setTokenId(ByteString.copyFrom(toBytes(lockToken)))
                                        .setTimestamp(19)
                                        .build())
                                .setLease(Lease.newBuilder()
                                        .setLeaderTime(LeaderTime.newBuilder()
                                                .setLeaderId(ByteString.copyFrom(toBytes(leaderId)))
                                                .setTime(888888888)
                                                .build())
                                        .setValidityNanos(123456789)
                                        .build())
                                .build())
                        .setValueAndMultipleStateUpdates(ByteString.copyFrom(lockWatchState))
                        .build()
                        .toByteArray())));
        ConjureStartTransactionsResponse response =
                autobatchingNamespacedConjureTimelockService.startTransactions(ConjureStartTransactionsRequest.builder()
                        .requestId(requestId)
                        .requestorId(requestorId)
                        .numTransactions(10)
                        .lastKnownVersion(Optional.of(ConjureIdentifiedVersion.of(leaderId, 55L)))
                        .build());
        assertThat(response.getTimestamps()).satisfies(partitionedTimestamps -> {
            assertThat(partitionedTimestamps.start()).isEqualTo(20L);
            assertThat(partitionedTimestamps.count()).isEqualTo(10);
            assertThat(partitionedTimestamps.interval()).isEqualTo(16);
        });
        assertThat(response.getImmutableTimestamp()).satisfies(lockImmutableTimestampResponse -> {
            assertThat(lockImmutableTimestampResponse.getLock().getRequestId()).isEqualTo(lockToken);
            assertThat(lockImmutableTimestampResponse.getImmutableTimestamp()).isEqualTo(19);
        });
        assertThat(response.getLease()).satisfies(lease -> {
            assertThat(lease.leaderTime().id().id()).isEqualTo(leaderId);
            assertThat(lease.leaderTime().currentTime().time()).isEqualTo(888888888);
            assertThat(lease.validity().toNanos()).isEqualTo(123456789);
        });
        assertThat(response.getLockWatchUpdate()).satisfies(lockWatchStateUpdate -> {
            lockWatchStateUpdate.accept(new Visitor<Void>() {
                @Override
                public Void visit(Success success) {
                    assertThat(success.lastKnownVersion()).isEqualTo(55);
                    assertThat(success.events()).hasSize(2).containsExactly(unlockEvent, createEvent);
                    return null;
                }

                @Override
                public Void visit(Snapshot snapshot) {
                    throw new SafeIllegalStateException("not expecting snapshot here");
                }
            });
        });

        ArgumentCaptor<TimeLockCommands> captor = ArgumentCaptor.forClass(TimeLockCommands.class);
        verify(namespacedConjureTimelockService).runCommands(captor.capture());
        TimeLockCommands commands = captor.getValue();
        assertThat(CommandSet.parseFrom(commands.get().asNewByteArray())).satisfies(cs -> {
            TransactionStartRequest onlyRequest = Iterables.getOnlyElement(cs.getTransactionStartRequestsList());
            assertThat(toUuid(onlyRequest.getRequestId().toByteArray())).isEqualTo(requestId);
            assertThat(toUuid(onlyRequest.getRequestorId().toByteArray())).isEqualTo(requestorId);
            assertThat(onlyRequest.getTransactionsToStart()).isEqualTo(10);
            assertThat(cs.getVersionsNeedingChecksList())
                    .containsExactly(LockWatchVersion.newBuilder()
                            .setLeaderId(ByteString.copyFrom(toBytes(leaderId)))
                            .setVersion(55)
                            .build());
            assertThat(cs.getNeedGenericLockWatchUpdate()).isFalse();
        });
    }

    private static UUID toUuid(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes, 0, 2 * Longs.BYTES).order(ByteOrder.BIG_ENDIAN);
        long mostSigBits = buf.getLong();
        long leastSigBits = buf.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    private static byte[] toBytes(UUID uuid) {
        return ByteBuffer.allocate(2 * Longs.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putLong(uuid.getMostSignificantBits())
                .putLong(uuid.getLeastSignificantBits())
                .array();
    }
}
