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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.palantir.atlasdb.autobatch.Autobatchers;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsRequest;
import com.palantir.atlasdb.timelock.api.ConjureGetFreshTimestampsResponse;
import com.palantir.atlasdb.timelock.api.ConjureIdentifiedVersion;
import com.palantir.atlasdb.timelock.api.ConjureLockRequest;
import com.palantir.atlasdb.timelock.api.ConjureLockResponse;
import com.palantir.atlasdb.timelock.api.ConjureLockResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureLockToken;
import com.palantir.atlasdb.timelock.api.ConjureLockTokenV2;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequest;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponse;
import com.palantir.atlasdb.timelock.api.ConjureRefreshLocksResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsRequest;
import com.palantir.atlasdb.timelock.api.ConjureStartTransactionsResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequest;
import com.palantir.atlasdb.timelock.api.ConjureUnlockRequestV2;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponse;
import com.palantir.atlasdb.timelock.api.ConjureUnlockResponseV2;
import com.palantir.atlasdb.timelock.api.ConjureWaitForLocksResponse;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsRequest;
import com.palantir.atlasdb.timelock.api.GetCommitTimestampsResponse;
import com.palantir.atlasdb.timelock.api.TimeLockCommandOutput;
import com.palantir.atlasdb.timelock.api.TimeLockCommands;
import com.palantir.common.time.NanoTime;
import com.palantir.conjure.java.lib.Bytes;
import com.palantir.conjure.java.serialization.ObjectMappers;
import com.palantir.lock.client.TimeLockOperation.Type;
import com.palantir.lock.generated.Command;
import com.palantir.lock.generated.Command.CommandOutput;
import com.palantir.lock.generated.Command.CommandSet;
import com.palantir.lock.generated.Command.Lease;
import com.palantir.lock.generated.Command.LockWatchVersion;
import com.palantir.lock.generated.Command.StartTransactionsResponse;
import com.palantir.lock.generated.Command.TimestampRange;
import com.palantir.lock.generated.Command.TokenSet;
import com.palantir.lock.generated.Command.TransactionStartRequest;
import com.palantir.lock.v2.ImmutableLease;
import com.palantir.lock.v2.ImmutablePartitionedTimestamps;
import com.palantir.lock.v2.LeaderTime;
import com.palantir.lock.v2.LeadershipId;
import com.palantir.lock.v2.LockImmutableTimestampResponse;
import com.palantir.lock.v2.LockToken;
import com.palantir.lock.v2.PartitionedTimestamps;
import com.palantir.lock.watch.LockWatchStateUpdate;
import com.palantir.lock.watch.LockWatchStateUpdate.Snapshot;
import com.palantir.lock.watch.LockWatchStateUpdate.Success;
import com.palantir.lock.watch.LockWatchStateUpdate.Visitor;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

// TODO (jkong): If we really wanted to do this in prod we probably want an A/B testing mechanism.
public class AutobatchingNamespacedConjureTimelockServiceImpl implements NamespacedConjureTimelockService {
    private final NamespacedConjureTimelockService delegate;
    private final DisruptorAutobatcher<TimeLockOperation, Object> autobatcher;

    public AutobatchingNamespacedConjureTimelockServiceImpl(NamespacedConjureTimelockService delegate) {
        this.delegate = delegate;
        this.autobatcher = Autobatchers.<TimeLockOperation, Object>independent(
                        elements -> processBatch(delegate, elements))
                .safeLoggablePurpose("timelock-mush")
                .build();
    }

    @Override
    public ConjureUnlockResponse unlock(ConjureUnlockRequest request) {
        ListenableFuture<Object> unlockFuture = autobatcher.apply(ImmutableTimeLockOperation.builder()
                .type(Type.UNLOCK_V1)
                .arguments(
                        request.getTokens().stream().map(t -> t.getRequestId()).collect(Collectors.toSet()))
                .build());
        try {
            Object returned = unlockFuture.get();
            Preconditions.checkState(returned instanceof Set, "Illegal return type");
            Set<UUID> returnedSet = (Set<UUID>) returned;
            return ConjureUnlockResponse.builder()
                    .tokens(returnedSet.stream().map(ConjureLockToken::of).collect(Collectors.toSet()))
                    .build();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public ConjureUnlockResponseV2 unlockV2(ConjureUnlockRequestV2 request) {
        ListenableFuture<Object> unlockFuture = autobatcher.apply(ImmutableTimeLockOperation.builder()
                .type(Type.UNLOCK_V2)
                .arguments(request.get().stream().map(ConjureLockTokenV2::get).collect(Collectors.toSet()))
                .build());
        try {
            Object returned = unlockFuture.get();
            Preconditions.checkState(returned instanceof Set, "Illegal return type");
            Set<UUID> returnedSet = (Set<UUID>) returned;
            return ConjureUnlockResponseV2.of(
                    returnedSet.stream().map(ConjureLockTokenV2::of).collect(Collectors.toSet()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public ConjureRefreshLocksResponse refreshLocks(ConjureRefreshLocksRequest request) {
        ListenableFuture<Object> refreshFuture = autobatcher.apply(ImmutableTimeLockOperation.builder()
                .type(Type.REFRESH_V1)
                .arguments(request.getTokens().stream()
                        .map(ConjureLockToken::getRequestId)
                        .collect(Collectors.toSet()))
                .build());
        try {
            Object returned = refreshFuture.get();
            Preconditions.checkState(returned instanceof ConjureRefreshLocksResponse, "Illegal return type");
            return ((ConjureRefreshLocksResponse) returned);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public ConjureRefreshLocksResponseV2 refreshLocksV2(ConjureRefreshLocksRequestV2 request) {
        ListenableFuture<Object> refreshFuture = autobatcher.apply(ImmutableTimeLockOperation.builder()
                .type(Type.REFRESH_V2)
                .arguments(request.get().stream().map(ConjureLockTokenV2::get).collect(Collectors.toSet()))
                .build());
        try {
            Object returned = refreshFuture.get();
            Preconditions.checkState(returned instanceof ConjureRefreshLocksResponseV2, "Illegal return type");
            return (ConjureRefreshLocksResponseV2) returned;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public ConjureWaitForLocksResponse waitForLocks(ConjureLockRequest request) {
        return delegate.waitForLocks(request);
    }

    @Override
    public ConjureLockResponse lock(ConjureLockRequest request) {
        return delegate.lock(request);
    }

    @Override
    public ConjureLockResponseV2 lockV2(ConjureLockRequest request) {
        return delegate.lockV2(request);
    }

    @Override
    public LeaderTime leaderTime() {
        ListenableFuture<Object> refreshFuture = autobatcher.apply(
                ImmutableTimeLockOperation.builder().type(Type.LEADER_TIME).build());
        try {
            Object returned = refreshFuture.get();
            Preconditions.checkState(returned instanceof LeaderTime, "Illegal return type");
            return (LeaderTime) returned;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public GetCommitTimestampsResponse getCommitTimestamps(GetCommitTimestampsRequest request) {
        ListenableFuture<Object> refreshFuture = autobatcher.apply(ImmutableTimeLockOperation.builder()
                .type(Type.GET_COMMIT_TIMESTAMPS)
                .arguments(request)
                .build());
        try {
            Object returned = refreshFuture.get();
            Preconditions.checkState(returned instanceof GetCommitTimestampsResponse, "Illegal return type");
            return (GetCommitTimestampsResponse) returned;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public ConjureGetFreshTimestampsResponse getFreshTimestamps(ConjureGetFreshTimestampsRequest request) {
        ListenableFuture<Object> timestampFuture = autobatcher.apply(ImmutableTimeLockOperation.builder()
                .type(Type.FRESH_TIMESTAMPS)
                .arguments(request.getNumTimestamps())
                .build());
        try {
            Object returned = timestampFuture.get();
            Preconditions.checkState(returned instanceof ConjureGetFreshTimestampsResponse, "Illegal return type");
            return (ConjureGetFreshTimestampsResponse) returned;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public ConjureStartTransactionsResponse startTransactions(ConjureStartTransactionsRequest request) {
        ListenableFuture<Object> transactionFuture = autobatcher.apply(ImmutableTimeLockOperation.builder()
                .type(Type.START_TRANSACTIONS)
                .arguments(request)
                .build());
        try {
            Object returned = transactionFuture.get();
            Preconditions.checkState(returned instanceof ConjureStartTransactionsResponse, "Illegal return type");
            return (ConjureStartTransactionsResponse) returned;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException) {
                throw (RuntimeException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public TimeLockCommandOutput runCommands(TimeLockCommands commands) {
        throw new SafeIllegalStateException("Don't run commands on me!");
    }

    private static void processBatch(
            NamespacedConjureTimelockService delegate, List<BatchElement<TimeLockOperation, Object>> elements) {
        // STEP 1: Parse the batch elements and assemble a request.
        CommandSet.Builder builder = CommandSet.newBuilder();
        TokenSet.Builder setToRefresh = TokenSet.newBuilder();
        TokenSet.Builder setToUnlock = TokenSet.newBuilder();
        Map<UUID, ConjureStartTransactionsRequest> startTransactionsRequestMap = new HashMap<>();
        long freshTimestamps = 0;
        for (BatchElement<TimeLockOperation, Object> batchElement : elements) {
            TimeLockOperation operation = batchElement.argument();

            switch (operation.type()) {
                case REFRESH_V1:
                case REFRESH_V2:
                    Set<UUID> tokensToRefresh = (Set<UUID>) operation.arguments()[0];
                    setToRefresh.addAllTokenId(tokensToRefresh.stream()
                            .map(uuid -> ByteString.copyFrom(toBytes(uuid)))
                            .collect(Collectors.toList()));
                    break;
                case UNLOCK_V1:
                case UNLOCK_V2:
                    Set<UUID> tokensToUnlock = (Set<UUID>) operation.arguments()[0];
                    setToUnlock.addAllTokenId(tokensToUnlock.stream()
                            .map(uuid -> ByteString.copyFrom(toBytes(uuid)))
                            .collect(Collectors.toList()));
                    break;
                case FRESH_TIMESTAMP:
                    freshTimestamps += 1;
                    break;
                case FRESH_TIMESTAMPS:
                    freshTimestamps += (int) operation.arguments()[0];
                    break;
                case LEADER_TIME:
                    builder.setNeedLeaderTime(true);
                    break;
                case GET_COMMIT_TIMESTAMPS:
                    GetCommitTimestampsRequest getCommitTimestampsRequest =
                            (GetCommitTimestampsRequest) operation.arguments()[0];
                    freshTimestamps += getCommitTimestampsRequest.getNumTimestamps();
                    if (getCommitTimestampsRequest.getLastKnownVersion().isPresent()) {
                        builder.addVersionsNeedingChecks(LockWatchVersion.newBuilder()
                                .setLeaderId(ByteString.copyFrom(toBytes(getCommitTimestampsRequest
                                        .getLastKnownVersion()
                                        .get()
                                        .getId())))
                                .setVersion(getCommitTimestampsRequest
                                        .getLastKnownVersion()
                                        .get()
                                        .getVersion())
                                .build());
                    } else {
                        builder.setNeedGenericLockWatchUpdate(true);
                    }
                    break;
                case START_TRANSACTIONS:
                    ConjureStartTransactionsRequest startTransactionsRequest =
                            (ConjureStartTransactionsRequest) operation.arguments()[0];
                    startTransactionsRequestMap.put(startTransactionsRequest.getRequestId(), startTransactionsRequest);
                    builder.addTransactionStartRequests(TransactionStartRequest.newBuilder()
                            .setRequestId(ByteString.copyFrom(toBytes(startTransactionsRequest.getRequestId())))
                            .setRequestorId(ByteString.copyFrom(toBytes(startTransactionsRequest.getRequestorId())))
                            .setTransactionsToStart(startTransactionsRequest.getNumTransactions())
                            .build());
                    // TODO (jkong): Look at this garbage
                    if (startTransactionsRequest.getLastKnownVersion().isPresent()) {
                        builder.addVersionsNeedingChecks(LockWatchVersion.newBuilder()
                                .setLeaderId(ByteString.copyFrom(toBytes(startTransactionsRequest
                                        .getLastKnownVersion()
                                        .get()
                                        .getId())))
                                .setVersion(startTransactionsRequest
                                        .getLastKnownVersion()
                                        .get()
                                        .getVersion())
                                .build());
                    } else {
                        builder.setNeedGenericLockWatchUpdate(true);
                    }
                    break;
                default:
                    throw new SafeIllegalStateException(
                            "Where am I? And what time is it?",
                            SafeArg.of("operationType", operation.type()),
                            UnsafeArg.of("operation", operation));
            }
        }

        CommandSet commandSet = builder.setToRefresh(setToRefresh.build())
                .setToUnlock(setToUnlock.build())
                .setTimestampsToRetrieve(Ints.checkedCast(freshTimestamps))
                .build();
        TimeLockCommandOutput timeLockCommandOutput =
                delegate.runCommands(TimeLockCommands.of(Bytes.from(commandSet.toByteArray())));
        CommandOutput commandOutput = tryParseCommandOutput(timeLockCommandOutput);

        TimestampRange rangeToGiveOut = commandOutput.getTimestamps();
        long offset = 0;
        Set<UUID> allRefreshedTokens = commandOutput.getRefreshed().getTokenIdList().stream()
                .map(ByteString::toByteArray)
                .map(AutobatchingNamespacedConjureTimelockServiceImpl::toUuid)
                .collect(Collectors.toSet());
        Set<UUID> allUnlockedTokens = commandOutput.getUnlocked().getTokenIdList().stream()
                .map(ByteString::toByteArray)
                .map(AutobatchingNamespacedConjureTimelockServiceImpl::toUuid)
                .collect(Collectors.toSet());
        Optional<StateUpdatePair> stateUpdatePair = commandOutput.hasValueAndMultipleStateUpdates()
                ? Optional.of(checkedDeserialize(
                        commandOutput.getValueAndMultipleStateUpdates().toByteArray()))
                : Optional.empty();
        Map<UUID, ConjureStartTransactionsResponse> startTransactionsResponseMap = Maps.newHashMap();

        for (StartTransactionsResponse startTransactionsResponse : commandOutput.getStartedTransactionsList()) {
            UUID key = toUuid(startTransactionsResponse.getRequestId().toByteArray());
            ConjureStartTransactionsRequest request = startTransactionsRequestMap.get(key);
            startTransactionsResponseMap.put(
                    key,
                    ConjureStartTransactionsResponse.builder()
                            .lockWatchUpdate(getCorrespondingLockWatchStateUpdate(
                                    request.getLastKnownVersion(), stateUpdatePair.get()))
                            .timestamps(toClientPartitionedTimestamps(startTransactionsResponse))
                            .immutableTimestamp(toClientImmutableTimestamps(startTransactionsResponse))
                            .lease(toClientLease(startTransactionsResponse.getLease()))
                            .build());
        }

        for (BatchElement<TimeLockOperation, Object> batchElement : elements) {
            // TODO (jkong): Complete the futures with the right things
            TimeLockOperation operation = batchElement.argument();
            switch (operation.type()) {
                case REFRESH_V1:
                    Set<UUID> tokensToRefresh = (Set<UUID>) operation.arguments()[0];
                    ConjureRefreshLocksResponse refreshResponse = ConjureRefreshLocksResponse.builder()
                            .lease(toClientLease(commandOutput.getRefreshLease()))
                            .refreshedTokens(Sets.union(tokensToRefresh, allRefreshedTokens).stream()
                                    .map(ConjureLockToken::of)
                                    .collect(Collectors.toSet()))
                            .build();
                    batchElement.result().set(refreshResponse);
                    break;
                case REFRESH_V2:
                    Set<UUID> tokensToRefreshV2 = (Set<UUID>) operation.arguments()[0];
                    ConjureRefreshLocksResponseV2 refreshResponseV2 = ConjureRefreshLocksResponseV2.builder()
                            .lease(toClientLease(commandOutput.getRefreshLease()))
                            .refreshedTokens(Sets.union(tokensToRefreshV2, allRefreshedTokens).stream()
                                    .map(ConjureLockTokenV2::of)
                                    .collect(Collectors.toSet()))
                            .build();
                    batchElement.result().set(refreshResponseV2);
                    break;
                case UNLOCK_V1:
                case UNLOCK_V2:
                    // The first person to unlock something is the person who got it!
                    Set<UUID> tokensToUnlock = (Set<UUID>) operation.arguments()[0];
                    Set<UUID> thingsIRemoved =
                            Sets.union(tokensToUnlock, allUnlockedTokens).immutableCopy();
                    tokensToUnlock.removeAll(thingsIRemoved);
                    batchElement.result().set(thingsIRemoved);
                    break;
                case FRESH_TIMESTAMP:
                    long targetTimestamp = rangeToGiveOut.getStartInclusive() + offset;
                    if (offset >= rangeToGiveOut.getNumGiven()) {
                        batchElement.result().setException(new SafeRuntimeException("bleh, ran out of timestamps"));
                    } else {
                        batchElement.result().set(targetTimestamp);
                        offset++;
                    }
                    break;
                case FRESH_TIMESTAMPS:
                    long timestampsAskedFor = (int) operation.arguments()[0];
                    long maxTarget = rangeToGiveOut.getStartInclusive() + offset + (timestampsAskedFor - 1);
                    if (maxTarget >= rangeToGiveOut.getNumGiven() + rangeToGiveOut.getStartInclusive()) {
                        batchElement.result().setException(new SafeRuntimeException("bleh, ran out of timestamps"));
                    } else {
                        long start = rangeToGiveOut.getStartInclusive() + offset;
                        batchElement.result().set(ConjureGetFreshTimestampsResponse.of(start, maxTarget));
                        offset += timestampsAskedFor;
                    }
                    break;
                case LEADER_TIME:
                    batchElement.result().set(toClientLeaderTime(commandOutput.getLeaderTime()));
                    break;
                case GET_COMMIT_TIMESTAMPS:
                    GetCommitTimestampsRequest getCommitTimestampsRequest =
                            (GetCommitTimestampsRequest) operation.arguments()[0];
                    // Handle timestamps
                    int requestedTimestamps = getCommitTimestampsRequest.getNumTimestamps();
                    long getCommitTimestampsEndBound =
                            rangeToGiveOut.getStartInclusive() + offset + (requestedTimestamps - 1);
                    ConjureGetFreshTimestampsResponse range;
                    if (getCommitTimestampsEndBound
                            >= rangeToGiveOut.getNumGiven() + rangeToGiveOut.getStartInclusive()) {
                        batchElement.result().setException(new SafeRuntimeException("bleh, ran out of timestamps"));
                    } else {
                        long start = rangeToGiveOut.getStartInclusive() + offset;
                        range = ConjureGetFreshTimestampsResponse.of(start, getCommitTimestampsEndBound);
                        offset += requestedTimestamps;

                        batchElement
                                .result()
                                .set(GetCommitTimestampsResponse.builder()
                                        .inclusiveLower(range.getInclusiveLower())
                                        .inclusiveUpper(range.getInclusiveUpper())
                                        .lockWatchUpdate(getCorrespondingLockWatchStateUpdate(
                                                getCommitTimestampsRequest.getLastKnownVersion(),
                                                stateUpdatePair.get()))
                                        .build());
                    }
                    break;
                case START_TRANSACTIONS:
                    // This is bizarre AF, but for this one let's operate on the response
                    ConjureStartTransactionsRequest startTransactionsRequest =
                            (ConjureStartTransactionsRequest) operation.arguments()[0];
                    batchElement
                            .result()
                            .set(startTransactionsResponseMap.get(startTransactionsRequest.getRequestId()));
                    break;
                default:
                    throw new SafeIllegalStateException("Unexpected type o_O");
            }
        }

        // All batch elements processed! We can go sleep.
    }

    private static LockImmutableTimestampResponse toClientImmutableTimestamps(
            StartTransactionsResponse startTransactionsResponse) {
        return LockImmutableTimestampResponse.of(
                startTransactionsResponse.getLockImmutableTimestampResponse().getTimestamp(),
                LockToken.of(toUuid(startTransactionsResponse
                        .getLockImmutableTimestampResponse()
                        .getTokenId()
                        .toByteArray())));
    }

    private static PartitionedTimestamps toClientPartitionedTimestamps(
            StartTransactionsResponse startTransactionsResponse) {
        return ImmutablePartitionedTimestamps.builder()
                .start(startTransactionsResponse.getPartitionedTimestamps().getStart())
                .interval(startTransactionsResponse.getPartitionedTimestamps().getInterval())
                .count(startTransactionsResponse.getPartitionedTimestamps().getCount())
                .build();
    }

    private static StateUpdatePair checkedDeserialize(byte[] byteArray) {
        try {
            return ObjectMappers.newSmileServerObjectMapper().readValue(byteArray, StateUpdatePair.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static LockWatchStateUpdate getCorrespondingLockWatchStateUpdate(
            Optional<ConjureIdentifiedVersion> ourVersion, StateUpdatePair stateUpdatePair) {
        if (ourVersion.isEmpty() || stateUpdatePair.oldestSuccess().isEmpty()) {
            return stateUpdatePair
                    .snapshot()
                    .orElseThrow(() -> new SafeRuntimeException(
                            "The server should have" + " given us a snapshot given our request, but it did not!"));
        }

        ConjureIdentifiedVersion presentVersion = ourVersion.get();
        LockWatchStateUpdate lastSuccess = stateUpdatePair.oldestSuccess().get();
        if (!presentVersion.getId().equals(lastSuccess.logId())
                || presentVersion.getVersion()
                        < lastSuccess.accept(new Visitor<Long>() {
                            @Override
                            public Long visit(Success success) {
                                return success.lastKnownVersion();
                            }

                            @Override
                            public Long visit(Snapshot snapshot) {
                                throw new SafeIllegalStateException("Last success should not be a snapshot!");
                            }
                        })) {
            return stateUpdatePair
                    .snapshot()
                    .orElseThrow(() -> new SafeRuntimeException(
                            "The server should have" + " given us a snapshot given our request, but it did not!"));
        }

        return lastSuccess.accept(new Visitor<>() {
            @Override
            public LockWatchStateUpdate visit(Success success) {
                return LockWatchStateUpdate.success(
                        success.logId(),
                        presentVersion.getVersion(),
                        success.events().stream()
                                .filter(e -> e.sequence() > presentVersion.getVersion())
                                .collect(Collectors.toList()));
            }

            @Override
            public LockWatchStateUpdate visit(Snapshot snapshot) {
                throw new SafeIllegalStateException("Last success should not be a snapshot!");
            }
        });
    }

    private static com.palantir.lock.v2.Lease toClientLease(Lease commandOutput) {
        return ImmutableLease.builder()
                .leaderTime(toClientLeaderTime(commandOutput.getLeaderTime()))
                .validity(Duration.ofNanos(commandOutput.getValidityNanos()))
                .build();
    }

    private static LeaderTime toClientLeaderTime(Command.LeaderTime commandLeaderTime) {
        return LeaderTime.of(
                LeadershipId.create(toUuid(commandLeaderTime.getLeaderId().toByteArray())),
                NanoTime.createForTests(commandLeaderTime.getTime()));
    }

    private static CommandOutput tryParseCommandOutput(TimeLockCommandOutput timeLockCommandOutput) {
        try {
            return CommandOutput.parseFrom(timeLockCommandOutput.get().asNewByteArray());
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
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
