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

package com.palantir.paxos;

import java.util.UUID;

public interface LeaderPinger {

    /**
     * Tries to find the host that has leadership identifier equal to {@code uuid}. If a host is found, it queries
     * whether it is the leader. Every failure case <b>must</b> result in {@code false}.
     *
     * @return whether a leader was found with leadership identifier being {@code uuid} or a different descriptive
     * result
     */
    LeaderPingResult pingLeaderWithUuid(UUID uuid);
}
