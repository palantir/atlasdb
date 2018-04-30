/*
 * (c) Copyright 2017 Palantir Technologies Inc. All rights reserved.
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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Lists;
import com.palantir.leader.proxy.ToggleableExceptionProxy;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.paxos.PaxosValue;

public class PaxosSynchronizerTest {
    @ClassRule
    public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final int NUM_NODES = 5;
    private static final String LEADER_ID = "foo";
    private static final byte[] PAXOS_DATA = new byte[0];
    private static final long SEQUENCE_ONE = 1L;
    private static final PaxosValue VALUE_ONE = new PaxosValue(LEADER_ID, SEQUENCE_ONE, PAXOS_DATA);
    private static final long SEQUENCE_TWO = 2L;
    private static final PaxosValue VALUE_TWO = new PaxosValue(LEADER_ID, SEQUENCE_TWO, PAXOS_DATA);
    private static final RuntimeException EXCEPTION = new RuntimeException("exception");

    private final List<PaxosLearner> learners = Lists.newArrayList();
    private final List<AtomicBoolean> failureToggles = Lists.newArrayList();

    private PaxosLearner ourLearner;

    @Before
    public void setUp() throws IOException {
        for (int i = 0; i < NUM_NODES; i++) {
            AtomicBoolean failureController = new AtomicBoolean(false);
            PaxosLearner learner = PaxosLearnerImpl.newLearner(TEMPORARY_FOLDER.newFolder().getPath());
            learners.add(ToggleableExceptionProxy.newProxyInstance(
                    PaxosLearner.class,
                    learner,
                    failureController,
                    EXCEPTION));
            failureToggles.add(failureController);
        }
        ourLearner = learners.get(0);
    }

    @Test
    public void synchronizeDoesNotThrowIfThereIsNoState() {
        PaxosSynchronizer.synchronizeLearner(ourLearner, learners);
    }

    @Test
    public void synchronizeUpdatesOurLearnerToValuesOthersHaveLearned() {
        learners.get(1).learn(SEQUENCE_ONE, VALUE_ONE);
        PaxosSynchronizer.synchronizeLearner(ourLearner, learners);
        assertThat(ourLearner.getGreatestLearnedValue()).isEqualTo(VALUE_ONE);
    }

    @Test
    public void synchronizeUpdatesUsToTheMostRecentData() {
        learners.get(1).learn(SEQUENCE_ONE, VALUE_ONE);
        learners.get(2).learn(SEQUENCE_TWO, VALUE_TWO);
        PaxosSynchronizer.synchronizeLearner(ourLearner, learners);
        assertThat(ourLearner.getLearnedValue(SEQUENCE_TWO)).isEqualTo(VALUE_TWO);
    }

    @Test
    public void synchronizeCanCopeWithDuplicateValues() {
        learners.stream()
                .filter(learner -> learner != ourLearner)
                .forEach(remoteLearner -> remoteLearner.learn(SEQUENCE_ONE, VALUE_ONE));
        PaxosSynchronizer.synchronizeLearner(ourLearner, learners);
        assertThat(ourLearner.getGreatestLearnedValue()).isEqualTo(VALUE_ONE);
    }

    @Test
    public void synchronizeUpdatesUsIfOurDataIsNotTheMostRecent() {
        ourLearner.learn(SEQUENCE_ONE, VALUE_ONE);
        learners.get(2).learn(SEQUENCE_TWO, VALUE_TWO);
        PaxosSynchronizer.synchronizeLearner(ourLearner, learners);
        assertThat(ourLearner.getLearnedValue(SEQUENCE_TWO)).isEqualTo(VALUE_TWO);
    }

    @Test
    public void synchronizeDoesNotUpdateUsToThePast() {
        learners.get(1).learn(SEQUENCE_ONE, VALUE_ONE);
        ourLearner.learn(SEQUENCE_TWO, VALUE_TWO);
        PaxosSynchronizer.synchronizeLearner(ourLearner, learners);
        assertThat(ourLearner.getGreatestLearnedValue()).isEqualTo(VALUE_TWO);
    }

    @Test
    public void synchronizePassesIfNodesCannotBeReached() {
        failureToggles.get(1).set(true);
        PaxosSynchronizer.synchronizeLearner(ourLearner, learners);
    }

    @Test
    public void synchronizePassesEvenIfWeAreIsolated() {
        failureToggles.forEach(atomicBoolean -> atomicBoolean.set(false));
        PaxosSynchronizer.synchronizeLearner(ourLearner, learners);
    }

    @Test
    public void synchronizeLearnsValuesOnABestEffortBasis() {
        learners.get(1).learn(SEQUENCE_ONE, VALUE_ONE);
        learners.get(2).learn(SEQUENCE_TWO, VALUE_TWO);
        failureToggles.get(2).set(true);
        PaxosSynchronizer.synchronizeLearner(ourLearner, learners);
        assertThat(ourLearner.getGreatestLearnedValue()).isEqualTo(VALUE_ONE);
    }
}
