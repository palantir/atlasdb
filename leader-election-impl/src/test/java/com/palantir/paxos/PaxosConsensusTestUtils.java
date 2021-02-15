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
package com.palantir.paxos;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.palantir.common.concurrent.PTExecutors;
import com.palantir.leader.LeaderElectionService;
import com.palantir.leader.LeaderElectionServiceBuilder;
import com.palantir.leader.PaxosKnowledgeEventRecorder;
import com.palantir.leader.proxy.SimulatingFailingServerProxy;
import com.palantir.leader.proxy.ToggleableExceptionProxy;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.apache.commons.io.FileUtils;

public final class PaxosConsensusTestUtils {

    private PaxosConsensusTestUtils() {}

    private static final long SERVER_DELAY_TIME_MS = 0;
    private static final String LOG_DIR = "testlogs/";
    private static final String LEARNER_DIR_PREFIX = LOG_DIR + "learner/";
    private static final String ACCEPTOR_DIR_PREFIX = LOG_DIR + "acceptor/";

    public static PaxosTestState setup(int numLeaders, int quorumSize) {
        List<LeaderElectionService> leaders = new ArrayList<>();
        List<PaxosAcceptor> acceptors = new ArrayList<>();
        List<PaxosLearner> learners = new ArrayList<>();
        List<AtomicBoolean> failureToggles = new ArrayList<>();
        ExecutorService executor = PTExecutors.newCachedThreadPool();

        DataSource sqliteDataSource = SqliteConnections.getPooledDataSource(getSqlitePath());

        RuntimeException exception = new SafeRuntimeException("mock server failure");
        SplittingPaxosStateLog.LegacyOperationMarkers noop = ImmutableLegacyOperationMarkers.builder()
                .markLegacyRead(() -> {})
                .markLegacyWrite(() -> {})
                .build();
        for (int i = 0; i < numLeaders; i++) {
            failureToggles.add(new AtomicBoolean(false));

            PaxosLearner learner = PaxosLearnerImpl.newSplittingLearner(
                    getLearnerStorageParameters(i, sqliteDataSource), noop, PaxosKnowledgeEventRecorder.NO_OP);
            learners.add(ToggleableExceptionProxy.newProxyInstance(
                    PaxosLearner.class, learner, failureToggles.get(i), exception));

            PaxosAcceptor acceptor = PaxosAcceptorImpl.newSplittingAcceptor(
                    getAcceptorStorageParameters(i, sqliteDataSource), noop, Optional.empty());
            acceptors.add(ToggleableExceptionProxy.newProxyInstance(
                    PaxosAcceptor.class, acceptor, failureToggles.get(i), exception));
        }

        PaxosAcceptorNetworkClient acceptorNetworkClient = SingleLeaderAcceptorNetworkClient.createLegacy(
                acceptors, quorumSize, Maps.toMap(acceptors, $ -> executor), PaxosConstants.CANCEL_REMAINING_CALLS);

        for (int i = 0; i < numLeaders; i++) {
            UUID leaderUuid = UUID.randomUUID();

            PaxosLearner ourLearner = learners.get(i);
            List<PaxosLearner> remoteLearners = learners.stream()
                    .filter(learner -> !learner.equals(ourLearner))
                    .collect(ImmutableList.toImmutableList());
            PaxosLearnerNetworkClient learnerNetworkClient = SingleLeaderLearnerNetworkClient.createLegacy(
                    ourLearner,
                    remoteLearners,
                    quorumSize,
                    Maps.toMap(learners, $ -> executor),
                    PaxosConstants.CANCEL_REMAINING_CALLS);

            LeaderElectionService leader = new LeaderElectionServiceBuilder()
                    .leaderUuid(leaderUuid)
                    .pingRate(Duration.ZERO)
                    .randomWaitBeforeProposingLeadership(Duration.ZERO)
                    .leaderAddressCacheTtl(Duration.ZERO)
                    .knowledge(ourLearner)
                    .acceptorClient(acceptorNetworkClient)
                    .learnerClient(learnerNetworkClient)
                    .leaderPinger(new SingleLeaderPinger(
                            ImmutableMap.of(), Duration.ZERO, leaderUuid, true, Optional.empty()))
                    .build();
            leaders.add(SimulatingFailingServerProxy.newProxyInstance(
                    LeaderElectionService.class, leader, SERVER_DELAY_TIME_MS, failureToggles.get(i)));
        }

        return new PaxosTestState(leaders, learners, failureToggles, executor);
    }

    public static void teardown(PaxosTestState state) throws Exception {
        try {
            ExecutorService executor = state.getExecutor();
            executor.shutdownNow();
            boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
            if (!terminated) {
                throw new SafeIllegalStateException("Some threads are still hanging around!"
                        + " Can't proceed or they might corrupt future tests.");
            }
        } finally {
            FileUtils.deleteDirectory(new File(LOG_DIR));
        }
    }

    private static PaxosStorageParameters getLearnerStorageParameters(int num, DataSource sqliteDataSource) {
        return ImmutablePaxosStorageParameters.builder()
                .fileBasedLogDirectory(getLearnerLogDir(num))
                .sqliteDataSource(sqliteDataSource)
                .namespaceAndUseCase(ImmutableNamespaceAndUseCase.of(Client.of(Integer.toString(num)), "learner"))
                .build();
    }

    private static PaxosStorageParameters getAcceptorStorageParameters(int num, DataSource sqliteDataSource) {
        return ImmutablePaxosStorageParameters.builder()
                .fileBasedLogDirectory(getAcceptorLogDir(num))
                .sqliteDataSource(sqliteDataSource)
                .namespaceAndUseCase(ImmutableNamespaceAndUseCase.of(Client.of(Integer.toString(num)), "acceptor"))
                .build();
    }

    private static String getLearnerLogDir(int dir) {
        return LEARNER_DIR_PREFIX + dir;
    }

    private static String getAcceptorLogDir(int dir) {
        return ACCEPTOR_DIR_PREFIX + dir;
    }

    private static Path getSqlitePath() {
        return Paths.get(LOG_DIR, "sqlite");
    }
}
