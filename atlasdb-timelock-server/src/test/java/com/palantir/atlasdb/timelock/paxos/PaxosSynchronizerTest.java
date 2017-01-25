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

import java.io.IOException;
import java.util.List;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.Lists;
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

    private final List<PaxosLearner> learners = Lists.newArrayList();

    private PaxosLearner ourLearner;

    @Before
    public void setUp() throws IOException {
        for (int i = 0; i < NUM_NODES; i++) {
            learners.add(PaxosLearnerImpl.newLearner(TEMPORARY_FOLDER.newFolder().getPath()));
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
    public void synchronizeDoesNotUpdateUsIfWeHaveTheMostRecentData() {
        learners.get(1).learn(SEQUENCE_ONE, VALUE_ONE);
        ourLearner.learn(SEQUENCE_TWO, VALUE_TWO);
        PaxosSynchronizer.synchronizeLearner(ourLearner, learners);
        assertThat(ourLearner.getGreatestLearnedValue()).isEqualTo(VALUE_TWO);
    }
}
