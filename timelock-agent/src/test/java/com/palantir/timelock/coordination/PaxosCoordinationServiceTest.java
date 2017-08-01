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

package com.palantir.timelock.coordination;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.proxy.ToggleableExceptionProxy;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorImpl;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerImpl;
import com.palantir.paxos.PaxosProposer;
import com.palantir.paxos.PaxosProposerImpl;
import com.palantir.remoting2.tracing.Tracers;
import com.palantir.timelock.partition.Assignment;

public class PaxosCoordinationServiceTest {

    private static final int NUM_NODES = 5;

    private static final String LOG_DIR = "testlogs/";
    private static final String LEARNER_DIR_PREFIX = LOG_DIR + "learner/";
    private static final String ACCEPTOR_DIR_PREFIX = LOG_DIR + "acceptor/";

    private static final RuntimeException EXCEPTION = new RuntimeException("exception");

    private static final Assignment ASSIGNMENT_1 = Assignment.builder()
            .addMapping("1", "2")
            .build();
    private static final Assignment ASSIGNMENT_2 = Assignment.builder()
            .addMapping("3", "4")
            .build();

    private final ExecutorService executor = Tracers.wrap(PTExecutors.newCachedThreadPool());
    private final List<PaxosAcceptor> acceptors = Lists.newArrayList();
    private final List<PaxosLearner> learners = Lists.newArrayList();
    private final List<AtomicBoolean> failureToggles = Lists.newArrayList();

    private PaxosCoordinationService coordinationService;

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

        coordinationService = createCoordinationService(0);
    }

    @After
    public void tearDown() throws InterruptedException, IOException {
        try {
            executor.shutdownNow();
            boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
            if (!terminated) {
                throw new IllegalStateException(
                        "Some threads are still hanging around! Can't proceed or they might corrupt future tests.");
            }
        } finally {
            FileUtils.deleteDirectory(new File(LOG_DIR));
        }
    }

    @Test
    public void getsNopAssignmentIfNothingAgreedYet() {
        assertThat(coordinationService.getAssignment()).isEqualTo(Assignment.nopAssignment());
    }

    @Test
    public void canProposeAndReadNewAssignment() {
        coordinationService.proposeAssignment(ASSIGNMENT_1);
        assertThat(coordinationService.getAssignment()).isEqualTo(ASSIGNMENT_1);
    }

    @Test
    public void assignmentChangesPropagatedAcrossNodes() {
        coordinationService.proposeAssignment(ASSIGNMENT_1);
        PaxosCoordinationService service2 = createCoordinationService(1);
        assertThat(service2.getAssignment()).isEqualTo(ASSIGNMENT_1);
        service2.proposeAssignment(ASSIGNMENT_2);
        assertThat(coordinationService.getAssignment()).isEqualTo(ASSIGNMENT_2);
    }

    @Test
    public void proposeNewAssignmentFailsWithoutAllNodesUp() {
        failureToggles.get(1).set(true);
        assertThatThrownBy(() -> coordinationService.proposeAssignment(ASSIGNMENT_1))
                .isInstanceOf(ServiceNotAvailableException.class);
    }

    @Test
    public void proposeNewAssignmentCanSucceedAfterNodeRecovery() {
        failureToggles.get(1).set(true);
        assertThatThrownBy(() -> coordinationService.proposeAssignment(ASSIGNMENT_1))
                .isInstanceOf(ServiceNotAvailableException.class);
        failureToggles.get(1).set(false);

        coordinationService.proposeAssignment(ASSIGNMENT_1);
        assertThat(coordinationService.getAssignment()).isEqualTo(ASSIGNMENT_1);
        assertThat(createCoordinationService(1).getAssignment()).isEqualTo(ASSIGNMENT_1);
    }

    @Test
    public void canReadAssignmentWithoutQuorumIfUpToDate() {
        coordinationService.proposeAssignment(ASSIGNMENT_1);
        failureToggles.get(1).set(true);
        assertThat(coordinationService.getAssignment()).isEqualTo(ASSIGNMENT_1);
    }

    @Test
    public void canReadAssignmentWithAllOtherNodesDownIfUpToDate() {
        coordinationService.proposeAssignment(ASSIGNMENT_1);
        IntStream.range(1, NUM_NODES).forEach(index -> failureToggles.get(index).set(true));
        assertThat(coordinationService.getAssignment()).isEqualTo(ASSIGNMENT_1);
    }

    @Test
    public void handlesMultipleProposals() {
        List<CoordinationService> services =
                IntStream.range(0, NUM_NODES).mapToObj(this::createCoordinationService).collect(Collectors.toList());
        for (int i = 0; i < 100; i++) {
            Assignment assignmentToAdd = i % 2 == 0 ? ASSIGNMENT_1 : ASSIGNMENT_2;
            services.get(i % NUM_NODES).proposeAssignment(assignmentToAdd);
            services.forEach(service -> assertThat(service.getAssignment()).isEqualTo(assignmentToAdd));
        }
    }

    @Test
    public void handlesConcurrentProposals() {
        List<CoordinationService> services =
                IntStream.range(0, NUM_NODES).mapToObj(this::createCoordinationService).collect(Collectors.toList());

        List<Future<Void>> futures = Lists.newArrayList();
        for (int i = 0; i < 100; i++) {
            int currentRound = i;
            futures.add(executor.submit(() -> {
                services.get(currentRound % NUM_NODES).proposeAssignment(
                        currentRound % 2 == 0 ? ASSIGNMENT_1 : ASSIGNMENT_2);
                return null;
            }));
        }

        for (Future<Void> future : futures) {
            Futures.getUnchecked(future);
        }

        Assignment consensus = services.get(0).getAssignment();
        for (int i = 0; i < NUM_NODES; i++) {
            assertThat(services.get(i).getAssignment()).isEqualTo(consensus);
        }
    }

    private PaxosCoordinationService createCoordinationService(int nodeIndex) {
        PaxosProposer proposer = createPaxosProposer(nodeIndex);
        return createCoordinationService(nodeIndex, proposer);
    }

    private PaxosCoordinationService createCoordinationService(int nodeIndex, PaxosProposer proposer) {
        return new PaxosCoordinationService(
                proposer,
                acceptors.get(nodeIndex),
                learners.get(nodeIndex),
                acceptors,
                learners);
    }

    private PaxosProposer createPaxosProposer(int nodeIndex) {
        return PaxosProposerImpl.newProposer(
                learners.get(nodeIndex),
                ImmutableList.copyOf(acceptors),
                ImmutableList.copyOf(learners),
                NUM_NODES,
                UUID.randomUUID(),
                executor);
    }
}
