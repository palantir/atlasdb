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
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.NamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqliteConnections;
import com.palantir.paxos.SqlitePaxosStateLog;
import com.palantir.timelock.TimelockCorruptionTestConstants;
import com.palantir.timelock.history.LocalHistoryLoader;
import com.palantir.timelock.history.PaxosLogHistoryProvider;
import com.palantir.timelock.history.TimeLockPaxosHistoryProvider;
import com.palantir.timelock.history.models.AcceptorUseCase;
import com.palantir.timelock.history.models.LearnerUseCase;
import com.palantir.timelock.history.remote.TimeLockPaxosHistoryProviderResource;
import com.palantir.timelock.history.sqlite.SqlitePaxosStateLogHistory;
import java.util.List;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.immutables.value.Value;
import org.junit.rules.ExternalResource;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public final class TimeLockCorruptionTestSetup implements TestRule {
    private TemporaryFolder tempFolder = new TemporaryFolder();
    private DataSource localDataSource;
    private DataSource remoteDataSource1;
    private DataSource remoteDataSource2;
    private PaxosLogHistoryProvider paxosLogHistoryProvider;
    private LocalTimestampInvariantsVerifier localTimestampInvariantsVerifier;

    private StateLogComponents defaultLocalServer;
    private List<StateLogComponents> defaultRemoteServerList;

    @Override
    public Statement apply(Statement base, Description description) {
        return RuleChain.outerRule(tempFolder)
                .around(new ExternalResource() {
                    @Override
                    protected void before() {
                        try {
                            setup();
                        } catch (Throwable throwable) {
                            throw new RuntimeException("Failed on startup", throwable);
                        }
                    }

                    @Override
                    protected void after() {
                        // no op
                    }
                })
                .apply(base, description);
    }

    private void setup() throws Throwable {
        localDataSource = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.newFolder("randomFile1").toPath());
        remoteDataSource1 = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.newFolder("randomFile2").toPath());
        remoteDataSource2 = SqliteConnections.getDefaultConfiguredPooledDataSource(
                tempFolder.newFolder("randomFile3").toPath());

        defaultLocalServer = createLogComponentsForServer(localDataSource);
        defaultRemoteServerList = ImmutableList.of(
                createLogComponentsForServer(remoteDataSource1), createLogComponentsForServer(remoteDataSource2));
        paxosLogHistoryProvider = paxosLogHistoryProvider();
        localTimestampInvariantsVerifier = new LocalTimestampInvariantsVerifier(localDataSource);
    }

    private PaxosLogHistoryProvider paxosLogHistoryProvider() {
        return new PaxosLogHistoryProvider(
                localDataSource,
                defaultRemoteServerList.stream()
                        .map(StateLogComponents::dataSource)
                        .map(TimeLockCorruptionTestSetup::getHistoryProviderResource)
                        .collect(Collectors.toList()));
    }

    List<StateLogComponents> createStatLogForNamespaceAndUseCase(NamespaceAndUseCase namespaceAndUseCase) {
        return ImmutableList.of(
                createLogComponentsForServer(localDataSource, namespaceAndUseCase),
                createLogComponentsForServer(remoteDataSource1, namespaceAndUseCase),
                createLogComponentsForServer(remoteDataSource2, namespaceAndUseCase));
    }

    List<StateLogComponents> getDefaultServerList() {
        return ImmutableList.<StateLogComponents>builder()
                .add(defaultLocalServer)
                .addAll(defaultRemoteServerList)
                .build();
    }

    PaxosLogHistoryProvider getPaxosLogHistoryProvider() {
        return paxosLogHistoryProvider;
    }

    StateLogComponents getDefaultLocalServer() {
        return defaultLocalServer;
    }

    List<StateLogComponents> getDefaultRemoteServerList() {
        return defaultRemoteServerList;
    }

    public LocalTimestampInvariantsVerifier getLocalTimestampInvariantsVerifier() {
        return localTimestampInvariantsVerifier;
    }

    private static StateLogComponents createLogComponentsForServer(DataSource dataSource) {
        return createLogComponentsForServer(dataSource, TimelockCorruptionTestConstants.DEFAULT_NAMESPACE_AND_USE_CASE);
    }

    private static StateLogComponents createLogComponentsForServer(
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

        return StateLogComponents.builder()
                .dataSource(dataSource)
                .learnerLog(learnerLog)
                .acceptorLog(acceptorLog)
                .build();
    }

    private static TimeLockPaxosHistoryProvider getHistoryProviderResource(DataSource dataSource) {
        return TimeLockPaxosHistoryProviderResource.jersey(
                LocalHistoryLoader.create(SqlitePaxosStateLogHistory.create(dataSource)));
    }

    @Value.Immutable
    interface StateLogComponents {
        DataSource dataSource();

        PaxosStateLog<PaxosValue> learnerLog();

        PaxosStateLog<PaxosAcceptorState> acceptorLog();

        static ImmutableStateLogComponents.Builder builder() {
            return ImmutableStateLogComponents.builder();
        }
    }
}
