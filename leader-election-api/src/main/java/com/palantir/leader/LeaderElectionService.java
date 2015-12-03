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
package com.palantir.leader;

import java.io.Serializable;

import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;

public interface LeaderElectionService {
    public static interface LeadershipToken extends Serializable {
        public boolean sameAs(LeadershipToken o);
    }

    public static enum StillLeadingStatus {
        LEADING,
        NOT_LEADING,
        /**
         * We could not talk to enough nodes to determine if we were still leading.
         * However we have not observed reason to believe another node is the leader.
         * Usually we should just retry in this case.
         */
        NO_QUORUM;
    }

    /**
     * This method will block until this node becomes the leader and is supported by a quorum of nodes.
     *
     * @return a leadership token to be used with {@link #isStillLeading}
     * @throws InterruptedException
     */
    LeadershipToken blockOnBecomingLeader() throws InterruptedException;

    /**
     * This method actually ensures that there is a quorum of supporters that this node is still leading.
     * If this node cannot get a quorum that it is still the leader then this method will return false and this node
     * may not respond to requests and must get back in line by calling {@link #blockOnBecomingLeader()}.
     *
     * @param token leadership token
     * @return LEADING if the token is still the leader.
     */
    StillLeadingStatus isStillLeading(LeadershipToken token);

    /**
     * Cheaply get the network location of the currently suspected leader. This will not do any network
     * calls and is meant to be callable without major performance implications. The value it returns
     * is only a hint and may be unreliable. It can also return nothing if it doesn't suspect any
     * leader or can't cheaply find one.
     */
    Optional<HostAndPort> getSuspectedLeaderInMemory();
}
