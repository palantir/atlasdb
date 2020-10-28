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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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

public abstract class TimeLockCorruptionTestSetup {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private static final Client CLIENT = Client.of("client");
    private static final String USE_CASE = "useCase";
    protected static final NamespaceAndUseCase NAMESPACE_AND_USE_CASE =
            ImmutableNamespaceAndUseCase.of(CLIENT, USE_CASE);

    private DataSource localDataSource;
    private DataSource remoteDataSource1;
    private DataSource remoteDataSource2;

    protected StateLogComponents localStateLogComponents;
    protected List<StateLogComponents> remoteStateLogComponents;
    protected PaxosLogHistoryProvider paxosLogHistoryProvider;

    @Before
    public void setup() throws IOException {
        localDataSource = SqliteConnections.getPooledDataSource(
                tempFolder.newFolder("randomFile1").toPath());
        remoteDataSource1 = SqliteConnections.getPooledDataSource(
                tempFolder.newFolder("randomFile2").toPath());
        remoteDataSource2 = SqliteConnections.getPooledDataSource(
                tempFolder.newFolder("randomFile3").toPath());

        localStateLogComponents = createLogComponentsForServer(localDataSource);
        remoteStateLogComponents = ImmutableList.of(
                createLogComponentsForServer(remoteDataSource1), createLogComponentsForServer(remoteDataSource2));
        paxosLogHistoryProvider = new PaxosLogHistoryProvider(
                localStateLogComponents.dataSource(),
                remoteStateLogComponents.stream()
                        .map(StateLogComponents::serverHistoryProvider)
                        .collect(Collectors.toList()));
    }

    protected List<StateLogComponents> createStatLogComponentsForNamespaceAndUseCase(
            NamespaceAndUseCase namespaceAndUseCase) {
        return ImmutableList.of(
                createLogComponentsForServer(localDataSource, namespaceAndUseCase),
                createLogComponentsForServer(remoteDataSource1, namespaceAndUseCase),
                createLogComponentsForServer(remoteDataSource2, namespaceAndUseCase));
    }

    // utils
    protected Set<PaxosValue> writeLogsOnServer(StateLogComponents server, int start, int end) {
        return PaxosSerializationTestUtils.writeToLogs(server.acceptorLog(), server.learnerLog(), start, end);
    }

    protected StateLogComponents createLogComponentsForServer(DataSource dataSource) {
        return createLogComponentsForServer(dataSource, NAMESPACE_AND_USE_CASE);
    }

    protected StateLogComponents createLogComponentsForServer(
            DataSource dataSource, NamespaceAndUseCase namespaceAndUseCase) {

        Client client = namespaceAndUseCase.namespace();
        String paxosUseCase = namespaceAndUseCase.useCase();

        PaxosStateLog<PaxosValue> learnerLog = SqlitePaxosStateLog.create(
                ImmutableNamespaceAndUseCase.of(
                        client,
                        LearnerUseCase.createLearnerUseCase(paxosUseCase).value()),
                dataSource);

        PaxosStateLog<PaxosAcceptorState> acceptorLog = SqlitePaxosStateLog.create(
                ImmutableNamespaceAndUseCase.of(
                        client,
                        AcceptorUseCase.createAcceptorUseCase(paxosUseCase).value()),
                dataSource);

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

    protected void induceGreaterAcceptedValueCorruption(int corruptSeq) {
        induceGreaterAcceptedValueCorruption(localStateLogComponents, corruptSeq);
    }

    protected void induceGreaterAcceptedValueCorruption(StateLogComponents server, int corruptSeq) {
        PaxosSerializationTestUtils.writeAcceptorStateForLogAndRound(
                server.acceptorLog(),
                corruptSeq,
                Optional.of(PaxosSerializationTestUtils.createPaxosValueForRoundAndData(corruptSeq, corruptSeq + 1)));
    }

    protected void writeLogsOnLocalAndRemote(int startingLogSeq, int latestLogSequence) {
        writeLogsOnLocalAndRemote(
                ImmutableList.<StateLogComponents>builder()
                        .add(localStateLogComponents)
                        .addAll(remoteStateLogComponents)
                        .build(),
                startingLogSeq,
                latestLogSequence);
    }

    protected void writeLogsOnLocalAndRemote(
            List<StateLogComponents> servers, int startingLogSeq, int latestLogSequence) {
        servers.forEach(server -> writeLogsOnServer(server, startingLogSeq, latestLogSequence));
    }

    protected SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> getViolationsToNamespaceToUseCaseMultimap() {
        List<CompletePaxosHistoryForNamespaceAndUseCase> historyForAll = paxosLogHistoryProvider.getHistory();
        return HistoryAnalyzer.corruptionHealthReportForHistory(historyForAll).violatingStatusesToNamespaceAndUseCase();
    }

    protected void assertDetectedViolations(Set<CorruptionCheckViolation> detectedViolations) {
        assertDetectedViolations(detectedViolations, ImmutableSet.of(NAMESPACE_AND_USE_CASE));
    }

    protected void assertDetectedViolations(
            Set<CorruptionCheckViolation> detectedViolations,
            Set<NamespaceAndUseCase> namespaceAndUseCasesWithViolation) {
        SetMultimap<CorruptionCheckViolation, NamespaceAndUseCase> violationsToNamespaceToUseCaseMultimap =
                getViolationsToNamespaceToUseCaseMultimap();
        assertThat(violationsToNamespaceToUseCaseMultimap.keySet()).hasSameElementsAs(detectedViolations);
        assertThat(violationsToNamespaceToUseCaseMultimap.values())
                .hasSameElementsAs(namespaceAndUseCasesWithViolation);
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
