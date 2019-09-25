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

import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;

public enum PaxosUseCase {
    LEADER_FOR_ALL_CLIENTS(PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE),
    LEADER_FOR_EACH_CLIENT(PaxosTimeLockConstants.MULTI_LEADER_PAXOS_NAMESPACE),
    TIMESTAMP(PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE);

    PaxosUseCase(String useCasePath) {
        this.useCasePath = useCasePath;
    }

    private final String useCasePath;

    /*
        Although this has no compile time usages, this is used for serialisation/deserialisation via Jersey
        {@link QueryParam}.
     */
    public static PaxosUseCase fromString(String string) {
        switch(string) {
            case PaxosTimeLockConstants.LEADER_PAXOS_NAMESPACE:
                return LEADER_FOR_ALL_CLIENTS;
            case PaxosTimeLockConstants.MULTI_LEADER_PAXOS_NAMESPACE:
                return LEADER_FOR_EACH_CLIENT;
            case PaxosTimeLockConstants.CLIENT_PAXOS_NAMESPACE:
                return TIMESTAMP;
            default:
                throw new SafeIllegalArgumentException("unrecognized use case");
        }
    }

    @Override
    public String toString() {
        return useCasePath;
    }
}
