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
package com.palantir.atlasdb.timelock.paxos;

import com.palantir.paxos.Client;
import com.palantir.paxos.PaxosConstants;

public final class PaxosTimeLockConstants {
    public static final String LEARNER_SUBDIRECTORY_PATH = "learner";
    public static final String ACCEPTOR_SUBDIRECTORY_PATH = "acceptor";

    // This is not great, but needed to preserve backwards compatibility for Leader Election Service
    public static final String LEADER_ELECTION_NAMESPACE = "leader";

    public static final String INTERNAL_NAMESPACE = ".internal";
    public static final String BATCH_INTERNAL_NAMESPACE = ".batch";
    public static final String LEADER_PAXOS_NAMESPACE = "leaderPaxos";
    public static final String CLIENT_PAXOS_NAMESPACE = "clientPaxos";
    public static final String MULTI_LEADER_PAXOS_NAMESPACE = "multiLeaderPaxos";

    public static final Client LEGACY_PAXOS_AS_CLIENT = Client.of(LEADER_PAXOS_NAMESPACE);

    /**
     * Prefer {@link PaxosConstants#CANCEL_REMAINING_CALLS}.
     * @deprecated in favor of {@link PaxosConstants#CANCEL_REMAINING_CALLS}
     */
    @Deprecated
    public static final boolean CANCEL_REMAINING_CALLS = PaxosConstants.CANCEL_REMAINING_CALLS;

    private PaxosTimeLockConstants() {}
}
