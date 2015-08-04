package com.palantir.paxos;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.ImmutableList;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.PaxosLeaderElectionService;
import com.palantir.leader.PingableLeader;
import com.palantir.leader.proxy.SimulatingFailingServerProxy;
import com.palantir.leader.proxy.ToggleableExceptionProxy;

public final class PaxosConsensusTestUtils {

    private PaxosConsensusTestUtils() {
    }

    private static final long SERVER_DELAY_TIME_MS = 0;
    private static final String LOG_DIR = "testlogs/";
    private static final String LEARNER_DIR_PREFIX = LOG_DIR + "learner/";
    private static final String ACCEPTOR_DIR_PREFIX = LOG_DIR + "acceptor/";

    public static void setup(int numLeaders,
                             int quorumSize,
                             List<LeaderElectionService> leaders,
                             List<PaxosAcceptor> acceptors,
                             List<PaxosLearner> learners,
                             List<AtomicBoolean> failureToggles) {
        Executor executor = PTExecutors.newCachedThreadPool();

        RuntimeException e = new RuntimeException("mock server failure");
        for (int i = 0; i < numLeaders; i++) {
            failureToggles.add(new AtomicBoolean(false));

            PaxosLearner learner = PaxosLearnerImpl.newLearner(getLearnerLogDir(i));
            learners.add(ToggleableExceptionProxy.newProxyInstance(
                    PaxosLearner.class,
                    learner,
                    failureToggles.get(i),
                    e));

            PaxosAcceptor acceptor = PaxosAcceptorImpl.newAcceptor(getAcceptorLogDir(i));
            acceptors.add(ToggleableExceptionProxy.newProxyInstance(
                    PaxosAcceptor.class,
                    acceptor,
                    failureToggles.get(i),
                    e));
        }

        for (int i = 0; i < numLeaders; i++) {
            PaxosProposer proposer = PaxosProposerImpl.newProposer(
                    learners.get(i),
                    ImmutableList.<PaxosAcceptor> copyOf(acceptors),
                    ImmutableList.<PaxosLearner> copyOf(learners),
                    quorumSize,
                    executor);
            PaxosLeaderElectionService leader = new PaxosLeaderElectionService(
                    proposer,
                    learners.get(i),
                    new ArrayList<PingableLeader>(),
                    ImmutableList.<PaxosAcceptor> copyOf(acceptors),
                    ImmutableList.<PaxosLearner> copyOf(learners),
                    executor,
                    0L, 0L, 0L);
            leaders.add(SimulatingFailingServerProxy.newProxyInstance(
                    LeaderElectionService.class,
                    leader,
                    SERVER_DELAY_TIME_MS,
                    failureToggles.get(i)));
        }
    }

    public static void teardown() {
        try {
            FileUtils.deleteDirectory(new File(LOG_DIR));
        } catch (Exception e) {}
    }

    public static String getLearnerLogDir(int i) {
        return LEARNER_DIR_PREFIX + i;
    }

    public static String getAcceptorLogDir(int i) {
        return ACCEPTOR_DIR_PREFIX + i;
    }
}
