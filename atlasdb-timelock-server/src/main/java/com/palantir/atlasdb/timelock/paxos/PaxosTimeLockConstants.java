/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.timelock.paxos;

public final class PaxosTimeLockConstants {
    public static final String DEFAULT_LOG_DIRECTORY = "var/data/";
    public static final String LEARNER_SUBDIRECTORY_PATH = "/learner";
    public static final String ACCEPTOR_SUBDIRECTORY_PATH = "/acceptor";

    public static final String LEADER_NAMESPACE = "__leader";

    private PaxosTimeLockConstants() {
    }
}
