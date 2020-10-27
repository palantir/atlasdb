/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.corruption.detection;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.SetMultimap;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqliteConnections;
import com.palantir.paxos.SqlitePaxosStateLog;
import com.palantir.timelock.history.LocalHistoryLoader;
import com.palantir.timelock.history.PaxosLogHistoryProvider;
import com.palantir.timelock.history.TimeLockPaxosHistoryProvider;
import com.palantir.timelock.history.models.AcceptorUseCase;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.models.LearnerUseCase;
import com.palantir.timelock.history.remote.TimeLockPaxosHistoryProviderResource;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import com.palantir.timelock.history.utils.PaxosSerializationTestUtils;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.immutables.value.Value;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public abstract class CorruptionDetectionEteSetup {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Client CLIENT = Client.of("client");
    private static final String USE_CASE = "useCase";
    private static final String USE_CASE_LEARNER =
            LearnerUseCase.createLearnerUseCase(USE_CASE).value();
    private static final String USE_CASE_ACCEPTOR =
            AcceptorUseCase.createAcceptorUseCase(USE_CASE).value();

    protected StateLogComponents localStateLogComponents;
    protected List<StateLogComponents> remoteStateLogComponents;
    PaxosLogHistoryProvider paxosLogHistoryProvider;

    @Before
    public void setup() throws IOException {
        localStateLogComponents = createLogComponentsForServer("randomFile1");
        remoteStateLogComponents = ImmutableList.of(
                createLogComponentsForServer("randomFile2"), createLogComponentsForServer("randomFile3"));
        paxosLogHistoryProvider = new PaxosLogHistoryProvider(
                localStateLogComponents.dataSource(),
                remoteStateLogComponents.stream()
                        .map(StateLogComponents::serverHistoryProvider)
                        .collect(Collectors.toList()));
    }

    // utils
    public Set<PaxosValue> writeLogsOnServer(StateLogComponents server, int start, int end) {
        return PaxosSerializationTestUtils.writeToLogs(server.acceptorLog(), server.learnerLog(), start, end);
    }

    public StateLogComponents createLogComponentsForServer(String fileName) throws IOException {
        DataSource dataSource = SqliteConnections.getPooledDataSource(
                tempFolder.newFolder(fileName).toPath());
        PaxosStateLog<PaxosValue> learnerLog =
                SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE_LEARNER), dataSource);
        PaxosStateLog<PaxosAcceptorState> acceptorLog =
                SqlitePaxosStateLog.create(ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE_ACCEPTOR), dataSource);
        LocalHistoryLoader history = LocalHistoryLoader.create(SqlitePaxosStateLogHistory.create(dataSource));
        TimeLockPaxosHistoryProvider serverHistoryProvider = TimeLockPaxosHistoryProviderResource.jersey(history);
        return StateLogComponents.builder()
                .dataSource(dataSource)
                .learnerLog(learnerLog)
                .acceptorLog(acceptorLog)
                .history(history)
                .serverHistoryProvider(serverHistoryProvider)
                .build();
    }

    public void induceGreaterAcceptedValueCorruption(int startingLogSeq, int latestLogSequence, int corruptSeq) {
        writeLogsOnServer(localStateLogComponents, startingLogSeq, latestLogSequence);
        remoteStateLogComponents.stream()
                .forEach(server -> writeLogsOnServer(server, startingLogSeq, latestLogSequence));

        PaxosSerializationTestUtils.writeAcceptorStateForLogAndRound(
                localStateLogComponents.acceptorLog(),
                corruptSeq,
                Optional.of(PaxosSerializationTestUtils.createPaxosValueForRoundAndData(corruptSeq, corruptSeq + 1)));
    }

    public SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> getViolationsToNamespaceToUseCaseMultimap() {
        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = paxosLogHistoryProvider.getHistory();
        return HistoryAnalyzer.corruptionHealthReportForHistory(historyForAll).violatingStatusesToNamespaceAndUseCase();
    }

    @Value.Immutable
    interface StateLogComponents {
        DataSource dataSource();

        PaxosStateLog<PaxosValue> learnerLog();

        PaxosStateLog<PaxosAcceptorState> acceptorLog();

        LocalHistoryLoader history();

        TimeLockPaxosHistoryProvider serverHistoryProvider();

        static ImmutableStateLogComponents.Builder builder() {
            return ImmutableStateLogComponents.builder();
        }
    }
}
