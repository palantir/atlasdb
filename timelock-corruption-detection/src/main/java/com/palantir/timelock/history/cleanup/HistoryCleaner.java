/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.timelock.history.cleanup;

import com.google.common.collect.Sets;
import com.palantir.atlasdb.timelock.api.management.TimeLockManagementService;
import com.palantir.paxos.Client;
import com.palantir.paxos.ImmutableNamespaceAndUseCase;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosStateLogImpl;
import com.palantir.paxos.PaxosValue;
import com.palantir.paxos.SqlitePaxosStateLog;
import com.palantir.timelock.corruption.detection.CorruptionHealthReport;
import com.palantir.timelock.corruption.detection.HistoryAnalyzer;
import com.palantir.timelock.history.PaxosLogHistoryProvider;
import com.palantir.timelock.history.models.CompletePaxosHistoryForNamespaceAndUseCase;
import com.palantir.timelock.history.sqlite.LogDeletionMarker;
import com.palantir.tokens.auth.AuthHeader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.sql.DataSource;

public class HistoryCleaner {
    private static final AuthHeader AUTH_HEADER = AuthHeader.valueOf("Bearer omitted");
    public static final String LEARNER_SUBDIRECTORY_PATH = "learner";
    public static final String ACCEPTOR_SUBDIRECTORY_PATH = "acceptor";

    private final DataSource dataSource;
    private final PaxosLogHistoryProvider historyProvider;
    private final TimeLockManagementService timeLockManagementService;
    private final Path baseLogDirectory;
    private final LogDeletionMarker deletionMarker;

    public HistoryCleaner(
            DataSource dataSource,
            PaxosLogHistoryProvider historyProvider,
            TimeLockManagementService timeLockManagementService,
            Path baseLogDirectory) {
        this.dataSource = dataSource;
        this.historyProvider = historyProvider;
        this.timeLockManagementService = timeLockManagementService;
        this.baseLogDirectory = baseLogDirectory;
        this.deletionMarker = LogDeletionMarker.create(dataSource);
    }

    public CorruptionHealthReport cleanUpHistoryAndGetHealthReport() {
        List<CompletePaxosHistoryForNamespaceAndUseCase> history = historyProvider.getHistory();
        CorruptionHealthReport healthReport = HistoryAnalyzer.corruptionHealthReportForHistory(history);
        Set<Client> namespacesEligibleForCleanup = getClientsEligibleForCleanup(history, healthReport);

        // consensus out of the way
        achieveConsensus(
                namespacesEligibleForCleanup.stream().map(Client::value).collect(Collectors.toSet()));
        // actually truncate
        history.forEach(this::runCleanUpOnNamespace);

        return healthReport;
    }

    private void runCleanUpOnNamespace(CompletePaxosHistoryForNamespaceAndUseCase history) {
        long greatestSeqNumber = history.greatestSeqNumber();

        // todo snanda - check positioning
        // mark deletion
        deletionMarker.updateProgress(history.namespace(), history.useCase(), greatestSeqNumber);

        // actually deletes
        truncateLearnerLogsForNamespaceAndUseCase(history, greatestSeqNumber);
        truncateAcceptorLogsForNamespaceAndUseCase(history, greatestSeqNumber);
    }

    private Set<Client> getClientsEligibleForCleanup(
            List<CompletePaxosHistoryForNamespaceAndUseCase> history, CorruptionHealthReport healthReport) {
        Set<Client> allNamespaces = history.stream()
                .map(CompletePaxosHistoryForNamespaceAndUseCase::namespace)
                .collect(Collectors.toSet());
        Set<Client> corruptNamespaces = healthReport.corruptNamespaces();

        return Sets.difference(allNamespaces, corruptNamespaces).immutableCopy();
    }

    // todo snanda ah 3 rounds of non-sense
    void achieveConsensus(Set<String> namespaces) {
        // do this thrice so we can delete rounds upto the greatest seq number we have so far
        for (int i = 0; i < 3; i++) {
            timeLockManagementService.achieveConsensus(AUTH_HEADER, namespaces);
        }
    }

    void truncateLearnerLogsForNamespaceAndUseCase(CompletePaxosHistoryForNamespaceAndUseCase history, long ts) {
        // todo sudiksha - maybe wanna reuse the jdbi  connections?
        // sqlite shit
        Client client = history.namespace();
        PaxosStateLog<PaxosValue> sqlite = SqlitePaxosStateLog.create(
                ImmutableNamespaceAndUseCase.of(
                        client, history.physicalLearnerUseCase().value()),
                dataSource);
        sqlite.truncate(ts);

        // disk
        Path legacyDir = baseLogDirectory.resolve(client.value());
        Path learnerLogDir = Paths.get(legacyDir.toString(), LEARNER_SUBDIRECTORY_PATH);
        PaxosStateLog<PaxosValue> diskLogs = PaxosStateLogImpl.createFileBacked(learnerLogDir.toString());
        diskLogs.truncate(ts);
    }

    void truncateAcceptorLogsForNamespaceAndUseCase(CompletePaxosHistoryForNamespaceAndUseCase history, long ts) {
        // sqlite shit
        Client client = history.namespace();
        PaxosStateLog<PaxosAcceptorState> sqlite = SqlitePaxosStateLog.create(
                ImmutableNamespaceAndUseCase.of(
                        client, history.physicalAcceptorUseCase().value()),
                dataSource);
        sqlite.truncate(ts);

        // disk
        Path legacyDir = baseLogDirectory.resolve(client.value());
        Path learnerLogDir = Paths.get(legacyDir.toString(), ACCEPTOR_SUBDIRECTORY_PATH);
        PaxosStateLog<PaxosValue> diskLogs = PaxosStateLogImpl.createFileBacked(learnerLogDir.toString());
        diskLogs.truncate(ts);
    }
}
