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

import com.palantir.atlasdb.config.LeaderConfig;

import io.airlift.airline.Command;

@Command(name = "deletePaxosLog", description = "Delete the paxos log after restoring at least quorum leaders that lost their acceptor log")
public class DeletePaxosLog extends AbstractCommand {

    @Override
    public Integer call() {

        // get leader config
        LeaderConfig leaderConfig;
        try {
            leaderConfig = getServiceConfigModule().provideAtlasDbConfig().leader().get();
        } catch (IllegalStateException e) {
            System.err.println("Error: Config file is missing required leader block configuration.");
            return 1;
        }

        try {
            for(File log : leaderConfig.acceptorLogDir().listFiles()) {
                Files.delete(log.toPath());
            }
            for(File log : leaderConfig.learnerLogDir().listFiles()) {
                Files.delete(log.toPath());
            }
        } catch (IOException e) {
            System.err.println("Error: IOException thrown attempting to delete paxos log file: " + e.getMessage());
            return 1;
        }

        System.out.println("Log was sucessfully deleted.");
        return 0;
    }

}

