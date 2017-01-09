/**
 * Copyright 2017 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.timelock.paxos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.proxy.ToggleableExceptionProxy;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;

public class PaxosTimestampBoundStoreTest {
    private static final int NUM_NODES = 5;

    private static final String LOG_DIR = "testlogs/";
    private static final String LEARNER_DIR_PREFIX = LOG_DIR + "learner/";
    private static final String ACCEPTOR_DIR_PREFIX = LOG_DIR + "acceptor/";
    private static final long TIMESTAMP_1 = 100000L;
    private static final long TIMESTAMP_2 = 200000L;
    private static final long TIMESTAMP_3 = 300000L;

    private static final String STILL_RUNNING_MESSAGE =
            "Some threads are still hanging around! Can't proceed or they might corrupt future tests.";
    private static final RuntimeException EXCEPTION = new RuntimeException("exception");

    private final ExecutorService executor = PTExecutors.newCachedThreadPool();
    private final List<PaxosAcceptor> acceptors = Lists.newArrayList();
    private final List<PaxosLearner> learners = Lists.newArrayList();
    private final List<AtomicBoolean> failureToggles = Lists.newArrayList();

    private PaxosTimestampBoundStore store;

    @Before
    public void setUp() {
        for (int i = 0; i < NUM_NODES; i++) {
            AtomicBoolean failureController = new AtomicBoolean(false);
            PaxosAcceptor acceptor = PaxosAcceptorImpl.newAcceptor(ACCEPTOR_DIR_PREFIX + i);
            acceptors.add(ToggleableExceptionProxy.newProxyInstance(
                    PaxosAcceptor.class,
                    acceptor,
                    failureController,
                    EXCEPTION));
            PaxosLearner learner = PaxosLearnerImpl.newLearner(LEARNER_DIR_PREFIX + i);
            learners.add(ToggleableExceptionProxy.newProxyInstance(
                    PaxosLearner.class,
                    learner,
                    failureController,
                    EXCEPTION));
            failureToggles.add(failureController);
        }

        store = createPaxosTimestampBoundStore(0);
    }

    @After
    public void tearDown() throws InterruptedException, IOException {
        try {
            executor.shutdownNow();
            boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
            if (!terminated) {
                throw new IllegalStateException(STILL_RUNNING_MESSAGE);
            }
        } finally {
            FileUtils.deleteDirectory(new File(LOG_DIR));
        }
    }

    @Test
    public void timestampsBeginFromZero() {
        assertThat(store.getUpperLimit()).isEqualTo(0L);
    }

    @Test
    public void canStoreUpperLimit() {
        store.storeUpperLimit(TIMESTAMP_1);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
    }

    @Test
    public void throwsIfStoringLimitLesserThanUpperLimit() {
        store.storeUpperLimit(TIMESTAMP_2);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_2);
        assertThatThrownBy(() -> store.storeUpperLimit(TIMESTAMP_1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void canOperateWithMinorityOfNodesDown() {
        failureToggles.get(1).set(true);
        failureToggles.get(2).set(true);
        store.storeUpperLimit(TIMESTAMP_1);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
    }

    @Test
    public void throwsIfCannotObtainQuorum() {
        failureToggles.get(1).set(true);
        failureToggles.get(2).set(true);
        failureToggles.get(3).set(true);
        assertThatThrownBy(() -> store.getUpperLimit()).isInstanceOf(ServiceNotAvailableException.class);
    }

    @Test
    public void retriesProposeUntilSuccessful() throws Exception {
        store = createOnceFailingPaxosTimestampBoundStore(0);
        store.storeUpperLimit(TIMESTAMP_1);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
    }

    @Test
    public void throwsIfBoundUnexpectedlyChangedUnderUs() {
        PaxosTimestampBoundStore additionalStore = createPaxosTimestampBoundStore(1);
        additionalStore.storeUpperLimit(TIMESTAMP_1);
        assertThatThrownBy(() -> store.storeUpperLimit(TIMESTAMP_2))
                .isInstanceOf(MultipleRunningTimestampServiceError.class);
    }

    @Test
    public void canReadStateFromDistributedLogs() {
        PaxosTimestampBoundStore additionalStore = createPaxosTimestampBoundStore(1);
        additionalStore.storeUpperLimit(TIMESTAMP_1);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
        store.storeUpperLimit(TIMESTAMP_2);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_2);
    }

    @Test
    public void canReadStateThroughLeaderChanges() {
        PaxosTimestampBoundStore additionalStore1 = createPaxosTimestampBoundStore(1);
        PaxosTimestampBoundStore additionalStore2 = createPaxosTimestampBoundStore(0);

        store.storeUpperLimit(TIMESTAMP_1);
        assertThat(additionalStore1.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
        additionalStore1.storeUpperLimit(TIMESTAMP_2);
        assertThat(additionalStore2.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_2);
        additionalStore2.storeUpperLimit(TIMESTAMP_3);
        assertThat(additionalStore2.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_3);
    }

    @Test
    public void canGetAgreedInitialState() {
        PaxosTimestampBoundStore.SequenceAndBound sequenceAndBound = store.getAgreedState(0);
        assertThat(sequenceAndBound.getSeqId()).isEqualTo(0);
        assertThat(sequenceAndBound.getBound()).isEqualTo(0);
    }

    @Test
    public void canGetAgreedState() {
        store.storeUpperLimit(TIMESTAMP_1);
        PaxosTimestampBoundStore.SequenceAndBound sequenceAndBound = store.getAgreedState(1);
        assertThat(sequenceAndBound.getSeqId()).isEqualTo(1);
        assertThat(sequenceAndBound.getBound()).isEqualTo(TIMESTAMP_1);
    }

    @Test
    public void canSafelyGetAgreedStateFromPrehistory() {
        assertThat(store.getAgreedState(Long.MIN_VALUE).getBound()).isEqualTo(0);
    }

    @Test
    public void canGetAgreedStateAfterPartition() {
        failureToggles.get(1).set(true);
        store.storeUpperLimit(TIMESTAMP_1);
        store.storeUpperLimit(TIMESTAMP_2);
        PaxosTimestampBoundStore additionalStore = createPaxosTimestampBoundStore(1);
        failureToggles.get(1).set(false);

        assertThat(additionalStore.getAgreedState(3).getBound()).isEqualTo(TIMESTAMP_2);
    }

    @Test
    public void cannotGetAgreedStateFromTheFuture() {
        assertThatThrownBy(() -> store.getAgreedState(Long.MAX_VALUE).getBound())
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    public void canSafelyForceAgreedStateFromPrehistory() {
        assertThat(store.forceAgreedState(Long.MIN_VALUE, Long.MIN_VALUE).getBound()).isEqualTo(0);
    }

    @Test
    public void retriesForceAgreedStateUntilSuccessful() throws Exception {
        store = createOnceFailingPaxosTimestampBoundStore(0);
        store.forceAgreedState(1, TIMESTAMP_1);
        assertThat(store.getUpperLimit()).isGreaterThanOrEqualTo(TIMESTAMP_1);
    }

    private PaxosTimestampBoundStore createPaxosTimestampBoundStore(int nodeIndex) {
        PaxosProposer proposer = createPaxosProposer(nodeIndex);
        return createPaxosTimestampBoundStore(nodeIndex, proposer);
    }

    private PaxosTimestampBoundStore createOnceFailingPaxosTimestampBoundStore(int nodeIndex)
            throws PaxosRoundFailureException {
        PaxosProposer proposer = new OnceFailingPaxosProposer(createPaxosProposer(nodeIndex));
        return createPaxosTimestampBoundStore(nodeIndex, proposer);
    }

    private PaxosProposer createPaxosProposer(int nodeIndex) {
        return PaxosProposerImpl.newProposer(
                learners.get(nodeIndex),
                ImmutableList.copyOf(acceptors),
                ImmutableList.copyOf(learners),
                NUM_NODES / 2 + 1,
                executor);
    }

    private PaxosTimestampBoundStore createPaxosTimestampBoundStore(int nodeIndex, PaxosProposer proposer) {
        return new PaxosTimestampBoundStore(
                proposer,
                learners.get(nodeIndex),
                ImmutableList.copyOf(acceptors),
                ImmutableList.copyOf(learners));
    }

    private static class OnceFailingPaxosProposer implements PaxosProposer {
        private final PaxosProposer delegate;
        private boolean hasFailed = false;

        private OnceFailingPaxosProposer(PaxosProposer delegate) {
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
        public int getQuorumSize() {
            return delegate.getQuorumSize();
        }

        @Override
        public String getUUID() {
            return delegate.getUUID();
        }
    }
}
