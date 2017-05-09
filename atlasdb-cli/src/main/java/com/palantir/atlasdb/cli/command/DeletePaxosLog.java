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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.config.LeaderConfig;

import io.airlift.airline.Command;

@Command(name = "delete-paxos-log", description = DeletePaxosLog.description)
public class DeletePaxosLog extends AbstractCommand {
    static final String description = "Delete the paxos log after restoring at least quorum leaders that lost their acceptor log.  "
            + "This operation is destructive and if accidentally done while elected service is up, use the truncatePaxosLog command to recover";

    private static final OutputPrinter printer = new OutputPrinter(LoggerFactory.getLogger(DeletePaxosLog.class));

    @Override
    public Integer call() {
        Preconditions.checkState(isOffline(), "This CLI can only be run offline.");
        Preconditions.checkState(getRawAtlasDbConfig().leader().isPresent(),
                "This CLI can only be run against services configured to be a leader.");
        LeaderConfig leader = getRawAtlasDbConfig().leader().get();

        try {
            for(File log : leader.acceptorLogDir().listFiles()) {
                Files.delete(log.toPath());
            }
            for(File log : leader.learnerLogDir().listFiles()) {
                Files.delete(log.toPath());
            }
        } catch (IOException e) {
            printer.error("Exception thrown attempting to delete paxos log files: {}", e.getMessage());
            return 1;
        }

        printer.info("Paxos log was sucessfully deleted.");
        return 0;
    }

}

