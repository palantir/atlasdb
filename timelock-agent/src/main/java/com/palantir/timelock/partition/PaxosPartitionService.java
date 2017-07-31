/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.timelock.partition;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosRoundFailureException;
import com.palantir.paxos.PaxosValue;

public class PaxosPartitionService implements PartitionService {
    private final PaxosProposer proposer;
    private final PaxosLearner knowledge;
    private final List<PaxosAcceptor> acceptors;
    private final List<PaxosLearner> learners;

    private final Supplier<Assignment> partitioner;

    private AtomicReference<Assignment> reference;
    private SequenceAndAssignment agreedState;

    public PaxosPartitionService(
            PaxosProposer proposer,
            PaxosLearner knowledge,
            List<PaxosAcceptor> acceptors,
            List<PaxosLearner> learners,
            Supplier<Assignment> partitioner) {
        this.proposer = proposer;
        this.knowledge = knowledge;
        this.acceptors = acceptors;
        this.learners = learners;
        this.partitioner = partitioner;
    }

    @Override
    public Assignment getPartition() {
        Assignment assignment = reference.get();
        if (assignment != null) {
            return assignment;
        }
        // block for 5 ms and try again
        Uninterruptibles.sleepUninterruptibly(5, TimeUnit.MILLISECONDS);
        assignment = reference.get();
        if (assignment != null) {
            return assignment;
        }
        throw new ServiceNotAvailableException("The partition service seems to be deadlocked, please try again later");
    }

    public void updateAssignment() {
        // TODO (jkong): Computing nextPartition requires information from the Paxos
        Assignment nextPartition = partitioner.get();

        long newSeq = agreedState == null ? PaxosAcceptor.NO_LOG_ENTRY + 1 : agreedState.sequenceNumber() + 1;
        while (true) {
            try {
                proposer.propose(newSeq, new ObjectMapper().writeValueAsBytes(nextPartition));
                PaxosValue value = knowledge.getLearnedValue(newSeq);
                Assignment newAssignment = new ObjectMapper().readValue(value.getData(), Assignment.class);
            } catch (PaxosRoundFailureException e) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    @Value.Immutable
    interface SequenceAndAssignment {
        @Value.Parameter
        long sequenceNumber();

        @Value.Parameter
        Assignment assignment();
    }
}
