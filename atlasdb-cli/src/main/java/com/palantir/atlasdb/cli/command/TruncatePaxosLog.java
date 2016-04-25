/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.cli.command;

import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.config.LeaderConfig;
import com.palantir.atlasdb.factory.Leaders;
import com.palantir.leader.PaxosLeaderElectionService;
import com.palantir.paxos.PaxosAcceptor;
import com.palantir.paxos.PaxosAcceptorState;
import com.palantir.paxos.PaxosProposalId;
import com.palantir.paxos.PaxosStateLog;
import com.palantir.paxos.PaxosStateLogImpl;

import io.airlift.airline.Command;

@Command(name = "truncatePaxosLog", description = "Truncate the paxos log after restoring a leader that lost its acceptor log")
public class TruncatePaxosLog extends AbstractCommand {

    @Override
    public Integer call() {

        // get leader config
        LeaderConfig leaderConfig;
        try {
            leaderConfig = scm.provideAtlasDbConfig().leader().get();
        } catch (IllegalStateException e) {
            System.err.println("Error: Config file is missing required leader block configuration.");
            return 1;
        }

        // ensure acceptor log is empty
        PaxosStateLog<PaxosAcceptorState> paxosLog = new PaxosStateLogImpl<PaxosAcceptorState>(leaderConfig.acceptorLogDir().toString());
        long greatestLogEntry = paxosLog.getGreatestLogEntry();
        if (greatestLogEntry > PaxosAcceptor.NO_LOG_ENTRY) {
            System.err.println("Error: This log has been written to already, but this cli was designed to "
                    + "be ran against a new server that has never been started.");
            return 1;
        }

        // create paxos service
        PaxosLeaderElectionService leaderElectionService;
        try {
            leaderElectionService = (PaxosLeaderElectionService) Leaders.create(
                    scm.provideServicesConfig().sslSocketFactory(),
                    resource -> {},
                    leaderConfig);
        } catch (ClassCastException e) { //impossible currently as Paxos is only existing implementation
            System.err.println("Error: LeaderElectionService is not a Paxos implementation, which this cli is designed for.");
            return 1;
        }

        // get the greatest log according to other leaders
        int quorumSize = leaderConfig.quorumSize();
        ImmutableList<PaxosAcceptor> acceptors = leaderElectionService.getAcceptors();
        long maxGreatestLog = -1L;
        int numSuccesses = 0;
        for (PaxosAcceptor paxosAcceptor : acceptors) {
            try {
                long lastLogEntry = paxosAcceptor.getLatestSequencePreparedOrAccepted();
                maxGreatestLog = Math.max(maxGreatestLog, lastLogEntry);
                numSuccesses++;
            } catch (Exception e) {
                System.out.println("Warn: Failed to get last log entry from: " + paxosAcceptor);
            }
        }
        if (numSuccesses < quorumSize) {
            System.err.println("Error: Failed because we could not talk to quorum servers to truncate this log correctly."
                    + "  This error has likely occurred because you are recovering from having lost at least quorum number"
                    + " of servers.  In this case you should take all services down and run the deletePaxosLog CLI against each of them.");
            return 1;
        }

        // never accept any of the pre-existing promises; truncate
        PaxosAcceptorState neverAcceptState = PaxosAcceptorState.newState(new PaxosProposalId(Long.MAX_VALUE, UUID.randomUUID().toString()));
        paxosLog.writeRound(maxGreatestLog + 1, neverAcceptState);
        paxosLog.truncate(maxGreatestLog);
        if (paxosLog.getLeastLogEntry() != paxosLog.getGreatestLogEntry()) {
            System.err.println("Error: This log has been written to already, but this cli was designed to "
                    + "be ran against a new server that has never been started.");
            return 1;
        }

        System.out.println("Log was sucessfully truncated.");
        return 0;
    }

}
