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
package com.palantir.timelock;

public enum TimeLockStatus {
    ONE_LEADER("There is exactly one leader in the Paxos cluster."),
    NO_LEADER("There are no leaders in the Paxos cluster"),
    MULTIPLE_LEADERS("Multiple nodes in the Paxos cluster believe themselves to be the leader."),
    NO_QUORUM("Less than a quorum of nodes responded to a ping request."),
    PENDING_ELECTION("No leader election has been triggered yet. A leader is elected upon the registration"
            + " of the first client, so this might indicate that no clients have been registered yet.");

    private final String message;

    TimeLockStatus(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }
}
