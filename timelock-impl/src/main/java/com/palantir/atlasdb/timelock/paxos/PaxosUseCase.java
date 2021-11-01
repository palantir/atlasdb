/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.paxos;

import static com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE;
import static com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE;
import static com.palantir.atlasdb.timelock.paxos.PaxosTimeLockConstants.MULTI_LEADER_PAXOS_NAMESPACE;

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.paxos.Client;
import java.nio.file.Path;
import java.nio.file.Paths;

public enum PaxosUseCase {

    // <data-directory>/<client="leaderPaxos">/{acceptor/learner}
    LEADER_FOR_ALL_CLIENTS(LEADER_PAXOS_NAMESPACE, Paths.get("")) {
        @Override
        public Client resolveClient(Client _client) {
            return PSEUDO_LEADERSHIP_CLIENT;
        }
    },

    // <data-directory>/leaderPaxos/multiLeaderPaxos/<client>/{acceptor/learner}
    LEADER_FOR_EACH_CLIENT(
            MULTI_LEADER_PAXOS_NAMESPACE, Paths.get(LEADER_PAXOS_NAMESPACE, MULTI_LEADER_PAXOS_NAMESPACE)) {
        @Override
        public Client resolveClient(Client client) {
            return client;
        }
    },

    // <data-directory>/<client>/{acceptor/learner}
    TIMESTAMP(CLIENT_PAXOS_NAMESPACE, Paths.get("")) {
        @Override
        public Client resolveClient(Client _client) {
            throw new SafeIllegalArgumentException("timestamp paxos should not be resolving clients");
        }
    };

    PaxosUseCase(String useCasePath, Path relativeLogDirectory) {
        this.useCasePath = useCasePath;
        this.relativeLogDirectory = relativeLogDirectory;
    }

    public static final Client PSEUDO_LEADERSHIP_CLIENT = Client.of(PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE);

    private final String useCasePath;
    private final Path relativeLogDirectory;

    /**
     * Although this has no compile time usages, this is used for serialisation/deserialisation via Jersey
     * {@link javax.ws.rs.QueryParam}.
     */
    public static PaxosUseCase fromString(String string) {
        switch (string) {
            case LEADER_PAXOS_NAMESPACE:
                return LEADER_FOR_ALL_CLIENTS;
            case MULTI_LEADER_PAXOS_NAMESPACE:
                return LEADER_FOR_EACH_CLIENT;
            case CLIENT_PAXOS_NAMESPACE:
                return TIMESTAMP;
            default:
                throw new SafeIllegalArgumentException("unrecognized use case");
        }
    }

    @Override
    public String toString() {
        return useCasePath;
    }

    public Path logDirectoryRelativeToDataDirectory(Path dataDirectory) {
        return dataDirectory.resolve(relativeLogDirectory);
    }

    public abstract Client resolveClient(Client client);
}
