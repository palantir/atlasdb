/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.timelock.paxos;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.palantir.atlasdb.util.MetricsManagers;
import com.palantir.common.concurrent.CheckedRejectionExecutorService;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.common.streams.KeyedStream;
import com.palantir.leader.SuspectedNotCurrentLeaderException;
import com.palantir.leader.proxy.ToggleableExceptionProxy;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosConstants;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.SingleLeaderAcceptorNetworkClient;
import com.palantir.paxos.SingleLeaderLearnerNetworkClient;
import com.palantir.paxos.SqliteConnections;
import com.palantir.sls.versions.OrderableSlsVersion;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class PaxosTimestampBoundStoreTest {
    private static final int NUM_NODES = 5;
    private static final int QUORUM_SIZE = NUM_NODES / 2 + 1;
    private static final boolean UNBATCHED = false;
    private static final boolean BATCHED = true;

    private static final Client CLIENT = Client.of("client");
    private static final long TIMESTAMP_1 = 100000;
    private static final long TIMESTAMP_2 = 200000;
    private static final long TIMESTAMP_3 = 300000;
    private static final long FORTY_TWO = 42;
    private static final PaxosTimestampBoundStore.SequenceAndBound ONE_AND_FORTY_TWO =
            ImmutableSequenceAndBound.of(1, FORTY_TWO);

    private static final RuntimeException EXCEPTION = new RuntimeException("exception");

    private final ExecutorService executor = PTExecutors.newCachedThreadPool();
    private final List<PaxosLearner> learners = new ArrayList<>();
    private final List<AtomicBoolean> failureToggles = new ArrayList<>();
    private final Closer closer = Closer.create();

    public static List<Boolean> useBatches() {
        return List.of(UNBATCHED, BATCHED);
    }

    @TempDir
    public File temporaryFolder;

    private PaxosAcceptorNetworkClient acceptorClient;
    private List<PaxosLearnerNetworkClient> learnerClientsByNode;
    private PaxosTimestampBoundStore store;

    @AfterEach
    public void tearDown() throws InterruptedException, IOException {
        closer.close();
        executor.shutdownNow();
        boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
        if (!terminated) {
            throw new IllegalStateException(
                    "Some threads are still hanging around! Can't proceed or they might corrupt future tests.");
        }
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void timestampsBeginFromZero(boolean useBatch) {
        setup(useBatch);
        assertThat(store.getUpperLimit()).isEqualTo(0L);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void canStoreUpperLimit(boolean useBatch) {
        setup(useBatch);
        store.storeUpperLimit(TIMESTAMP_1);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void throwsIfStoringLimitLessThanUpperLimit(boolean useBatch) {
        setup(useBatch);
        store.storeUpperLimit(TIMESTAMP_2);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_2);
        assertThatThrownBy(() -> store.storeUpperLimit(TIMESTAMP_1)).isInstanceOf(IllegalArgumentException.class);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void canOperateWithMinorityOfNodesDown(boolean useBatch) {
        setup(useBatch);
        failureToggles.get(1).set(true);
        failureToggles.get(2).set(true);
        store.storeUpperLimit(TIMESTAMP_1);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void throwsIfCannotObtainQuorum(boolean useBatch) {
        setup(useBatch);
        failureToggles.get(1).set(true);
        failureToggles.get(2).set(true);
        failureToggles.get(3).set(true);
        assertThatThrownBy(() -> store.getUpperLimit()).isInstanceOf(ServiceNotAvailableException.class);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void canRecoverFromNotHavingQuorum(boolean useBatch) {
        setup(useBatch);
        store.storeUpperLimit(TIMESTAMP_1);
        failureToggles.get(1).set(true);
        failureToggles.get(2).set(true);
        failureToggles.get(3).set(true);
        assertThatThrownBy(() -> store.getUpperLimit()).isInstanceOf(ServiceNotAvailableException.class);
        failureToggles.get(3).set(false);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void retriesProposeUntilSuccessful(boolean useBatch) throws Exception {
        setup(useBatch);
        PaxosProposer wrapper = spy(new OnceFailingPaxosProposer(createPaxosProposer(0)));
        store = createPaxosTimestampBoundStore(0, wrapper);
        store.storeUpperLimit(TIMESTAMP_1);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
        verify(wrapper, times(2)).propose(anyLong(), any());
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void throwsSuspectedNotCurrentLeaderExceptionIfBoundUnexpectedlyChangedUnderUs(boolean useBatch) {
        setup(useBatch);
        PaxosTimestampBoundStore additionalStore = createPaxosTimestampBoundStore(1);
        additionalStore.storeUpperLimit(TIMESTAMP_1);
        assertThatThrownBy(() -> store.storeUpperLimit(TIMESTAMP_2))
                .isInstanceOf(SuspectedNotCurrentLeaderException.class);
        assertThatThrownBy(() -> store.storeUpperLimit(TIMESTAMP_2))
                .as("no further requests should be permitted after a SuspectedNotCurrentLeaderException")
                .isInstanceOf(SafeIllegalStateException.class)
                .hasMessage(
                        "Cannot store upper limit as leadership has been lost, or this store is no longer current.");
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void canReadStateFromDistributedLogs(boolean useBatch) {
        setup(useBatch);
        PaxosTimestampBoundStore additionalStore = createPaxosTimestampBoundStore(1);
        additionalStore.storeUpperLimit(TIMESTAMP_1);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
        store.storeUpperLimit(TIMESTAMP_2);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_2);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void canReadConsensusProposedByOtherNodes(boolean useBatch) {
        setup(useBatch);
        PaxosTimestampBoundStore additionalStore1 = createPaxosTimestampBoundStore(1);
        PaxosTimestampBoundStore additionalStore2 = createPaxosTimestampBoundStore(0);

        store.storeUpperLimit(TIMESTAMP_1);
        assertThat(additionalStore1.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
        additionalStore1.storeUpperLimit(TIMESTAMP_2 - 1);
        additionalStore1.storeUpperLimit(TIMESTAMP_2);
        assertThat(additionalStore2.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_2);
        additionalStore2.storeUpperLimit(TIMESTAMP_3 - 1);
        additionalStore2.storeUpperLimit(TIMESTAMP_3);
        assertThat(additionalStore2.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_3);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void canGetAgreedInitialState(boolean useBatch) {
        setup(useBatch);
        PaxosTimestampBoundStore.SequenceAndBound sequenceAndBound = store.getAgreedState(0);
        assertThat(sequenceAndBound.getSeqId()).isEqualTo(0);
        assertThat(sequenceAndBound.getBound()).isEqualTo(0);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void canGetAgreedState(boolean useBatch) {
        setup(useBatch);
        store.storeUpperLimit(TIMESTAMP_1);
        PaxosTimestampBoundStore.SequenceAndBound sequenceAndBound = store.getAgreedState(1);
        assertThat(sequenceAndBound.getSeqId()).isEqualTo(1);
        assertThat(sequenceAndBound.getBound()).isEqualTo(TIMESTAMP_1);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void canSafelyGetAgreedStateFromPrehistory(boolean useBatch) {
        setup(useBatch);
        assertThat(store.getAgreedState(Long.MIN_VALUE).getBound()).isEqualTo(0);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void canGetAgreedStateAfterNodeDown(boolean useBatch) {
        setup(useBatch);
        int nodeId = 1;
        PaxosTimestampBoundStore additionalStore = createPaxosTimestampBoundStore(nodeId);
        failureToggles.get(nodeId).set(true);
        store.storeUpperLimit(TIMESTAMP_1);
        failureToggles.get(nodeId).set(false);

        assertThat(additionalStore.getAgreedState(2).getBound()).isEqualTo(TIMESTAMP_1);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void cannotGetAgreedStateFromTheFuture(boolean useBatch) {
        setup(useBatch);
        assertThatThrownBy(() -> store.getAgreedState(Long.MAX_VALUE)).isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void canSafelyForceAgreedStateFromPrehistory(boolean useBatch) {
        setup(useBatch);
        assertThat(store.forceAgreedState(Long.MIN_VALUE, Long.MIN_VALUE).getBound())
                .isEqualTo(0);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void canForceAgreedState(boolean useBatch) {
        setup(useBatch);
        assertThat(store.forceAgreedState(1, FORTY_TWO)).isEqualTo(ONE_AND_FORTY_TWO);
        assertThat(store.getAgreedState(1)).isEqualTo(ONE_AND_FORTY_TWO);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void forceAgreedStateCanBeUsedToGainKnowledge(boolean useBatch) {
        setup(useBatch);
        assertThat(store.forceAgreedState(1, FORTY_TWO)).isEqualTo(ONE_AND_FORTY_TWO);

        PaxosTimestampBoundStore additionalStore = createPaxosTimestampBoundStore(1);
        assertThat(additionalStore.forceAgreedState(1, null)).isEqualTo(ONE_AND_FORTY_TWO);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void forceAgreedStateReturnsFirstForcedValue(boolean useBatch) {
        setup(useBatch);
        assertThat(store.forceAgreedState(1, FORTY_TWO)).isEqualTo(ONE_AND_FORTY_TWO);
        assertThat(store.forceAgreedState(1, 1L)).isEqualTo(ONE_AND_FORTY_TWO);
        assertThat(store.getAgreedState(1)).isEqualTo(ONE_AND_FORTY_TWO);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void forceAgreedStateOperatesAtSequenceNumberLevel(boolean useBatch) {
        setup(useBatch);
        long fortyThree = FORTY_TWO + 1;
        assertThat(store.forceAgreedState(1, FORTY_TWO)).isEqualTo(ONE_AND_FORTY_TWO);
        assertThat(store.forceAgreedState(0, fortyThree)).isEqualTo(ImmutableSequenceAndBound.of(0, fortyThree));
        assertThat(store.getAgreedState(0)).isEqualTo(ImmutableSequenceAndBound.of(0, fortyThree));
        assertThat(store.getAgreedState(1)).isEqualTo(ONE_AND_FORTY_TWO);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void forceAgreedStateThrowsIfNoStateWasAgreedUpon(boolean useBatch) {
        setup(useBatch);
        assertThatThrownBy(() -> store.forceAgreedState(1, null)).isInstanceOf(NullPointerException.class);
    }

    @ParameterizedTest
    @MethodSource("useBatches")
    public void retriesForceAgreedStateUntilSuccessful(boolean useBatch) throws Exception {
        setup(useBatch);
        PaxosProposer wrapper = spy(new OnceFailingPaxosProposer(createPaxosProposer(0)));
        store = createPaxosTimestampBoundStore(0, wrapper);
        store.forceAgreedState(1, TIMESTAMP_1);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
        verify(wrapper, times(2)).propose(anyLong(), any(byte[].class));
    }

    public void setup(boolean useBatch) {
        List<PaxosAcceptor> acceptors = new ArrayList<>();
        List<BatchPaxosAcceptor> batchPaxosAcceptors = new ArrayList<>();
        List<BatchPaxosLearner> batchPaxosLearners = new ArrayList<>();

        for (int i = 0; i < NUM_NODES; i++) {
            String root = temporaryFolder.getAbsolutePath();
            LocalPaxosComponents components = LocalPaxosComponents.createWithAsyncMigration(
                    TimelockPaxosMetrics.of(PaxosUseCase.TIMESTAMP, MetricsManagers.createForTests()),
                    PaxosUseCase.TIMESTAMP,
                    Paths.get(root, i + "legacy"),
                    SqliteConnections.getDefaultConfiguredPooledDataSource(Paths.get(root, i + "sqlite")),
                    UUID.randomUUID(),
                    true,
                    OrderableSlsVersion.valueOf("0.0.0"),
                    false);

            AtomicBoolean failureController = new AtomicBoolean(false);
            failureToggles.add(failureController);

            learners.add(ToggleableExceptionProxy.newProxyInstance(
                    PaxosLearner.class, components.learner(CLIENT), failureController, EXCEPTION));

            acceptors.add(ToggleableExceptionProxy.newProxyInstance(
                    PaxosAcceptor.class, components.acceptor(CLIENT), failureController, EXCEPTION));

            BatchPaxosAcceptor batchAcceptor = new LocalBatchPaxosAcceptor(components, new AcceptorCacheImpl());
            batchPaxosAcceptors.add(ToggleableExceptionProxy.newProxyInstance(
                    BatchPaxosAcceptor.class, batchAcceptor, failureController, EXCEPTION));

            BatchPaxosLearner batchLearner = new LocalBatchPaxosLearner(components);
            batchPaxosLearners.add(ToggleableExceptionProxy.newProxyInstance(
                    BatchPaxosLearner.class, batchLearner, failureController, EXCEPTION));
        }

        if (useBatch) {
            setupAcceptorClientAndLearnerClientsByNodeUsingBatch(batchPaxosAcceptors, batchPaxosLearners);
        } else {
            setupAcceptorClientAndLearnerClientsByNodeWithoutUsingBatch(acceptors);
        }
        store = createPaxosTimestampBoundStore(0);
    }

    public void setupAcceptorClientAndLearnerClientsByNodeUsingBatch(
            List<BatchPaxosAcceptor> batchPaxosAcceptors, List<BatchPaxosLearner> batchPaxosLearners) {
        AutobatchingPaxosAcceptorNetworkClientFactory acceptorNetworkClientFactory =
                AutobatchingPaxosAcceptorNetworkClientFactory.create(
                        batchPaxosAcceptors,
                        KeyedStream.of(batchPaxosAcceptors.stream())
                                .map($ -> new CheckedRejectionExecutorService(executor))
                                .collectToMap(),
                        QUORUM_SIZE);
        acceptorClient = acceptorNetworkClientFactory.paxosAcceptorForClient(CLIENT);

        List<AutobatchingPaxosLearnerNetworkClientFactory> learnerNetworkClientFactories = batchPaxosLearners.stream()
                .map(localLearner -> LocalAndRemotes.of(
                        localLearner,
                        batchPaxosLearners.stream()
                                .filter(remoteLearners -> remoteLearners != localLearner)
                                .collect(toList())))
                .map(localAndRemotes -> AutobatchingPaxosLearnerNetworkClientFactory.createForTests(
                        localAndRemotes, executor, QUORUM_SIZE))
                .collect(toList());

        learnerClientsByNode = learnerNetworkClientFactories.stream()
                .map(factory -> factory.paxosLearnerForClient(CLIENT))
                .collect(toList());

        closer.register(acceptorNetworkClientFactory);
        learnerNetworkClientFactories.forEach(closer::register);
    }

    public void setupAcceptorClientAndLearnerClientsByNodeWithoutUsingBatch(List<PaxosAcceptor> acceptors) {
        acceptorClient = SingleLeaderAcceptorNetworkClient.createLegacy(
                acceptors, QUORUM_SIZE, Maps.toMap(acceptors, $ -> executor), PaxosConstants.CANCEL_REMAINING_CALLS);

        learnerClientsByNode = learners.stream()
                .map(learner -> SingleLeaderLearnerNetworkClient.createLegacy(
                        learner,
                        learners.stream()
                                .filter(otherLearners -> otherLearners != learner)
                                .collect(toList()),
                        QUORUM_SIZE,
                        Maps.toMap(learners, $ -> executor),
                        PaxosConstants.CANCEL_REMAINING_CALLS))
                .collect(toList());
    }

    private PaxosTimestampBoundStore createPaxosTimestampBoundStore(int nodeIndex) {
        PaxosProposer proposer = createPaxosProposer(nodeIndex);
        return createPaxosTimestampBoundStore(nodeIndex, proposer);
    }

    private PaxosTimestampBoundStore createPaxosTimestampBoundStore(int nodeIndex, PaxosProposer proposer) {
        return new PaxosTimestampBoundStore(
                proposer, learners.get(nodeIndex), acceptorClient, learnerClientsByNode.get(nodeIndex), 1000L);
    }

    private PaxosProposer createPaxosProposer(int nodeIndex) {
        return PaxosProposerImpl.newProposer(acceptorClient, learnerClientsByNode.get(nodeIndex), UUID.randomUUID());
    }

    private static class OnceFailingPaxosProposer implements PaxosProposer {
        private final PaxosProposer delegate;
        private boolean hasFailed = false;

        OnceFailingPaxosProposer(PaxosProposer delegate) {
            this.delegate = delegate;
        }

        @Override
        public byte[] propose(long seq, @Nullable byte[] proposalValue) throws PaxosRoundFailureException {
            if (hasFailed) {
                return delegate.propose(seq, proposalValue);
            }
            hasFailed = true;
            throw new PaxosRoundFailureException("paxos fail");
        }

        @Override
        public byte[] proposeAnonymously(long seq, @Nullable byte[] proposalValue) throws PaxosRoundFailureException {
            if (hasFailed) {
                return delegate.proposeAnonymously(seq, proposalValue);
            }
            hasFailed = true;
            throw new PaxosRoundFailureException("paxos fail");
        }

        @Override
        public String getUuid() {
            return delegate.getUuid();
        }
    }
}
