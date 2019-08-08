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
package com.palantir.leader;

import java.io.Serializable;
import java.util.Optional;
import java.util.Set;

public interface LeaderElectionService {
    interface LeadershipToken extends Serializable {
        boolean sameAs(LeadershipToken token);
    }

    enum StillLeadingStatus {
        LEADING,
        NOT_LEADING,
        /**
         * We could not talk to enough nodes to determine if we were still leading.
         * However we have not observed reason to believe another node is the leader.
         * Usually we should just retry in this case.
         */
        NO_QUORUM
    }

    /**
     * This method will block until this node becomes the leader and is supported by a quorum of nodes.
     *
     * @return a leadership token to be used with {@link #isStillLeading}
     */
    LeadershipToken blockOnBecomingLeader() throws InterruptedException;

    /**
     * Returns a leadership token iff this node is currently the leader. This method may involve network
     * calls to ensure that the token is valid at the time of invocation, but will return immediately if
     * this node is not the last known leader.
     */
    Optional<LeadershipToken> getCurrentTokenIfLeading();

    /**
     * This method actually ensures that there is a quorum of supporters that this node is still leading.
     * If this node cannot get a quorum that it is still the leader then this method will return false and this node
     * may not respond to requests and must get back in line by calling {@link #blockOnBecomingLeader()}.
     *
     * @param token leadership token
     * @return LEADING if the token is still the leader
     */
    StillLeadingStatus isStillLeading(LeadershipToken token);

    /**
     * Get the set of potential leaders known by this leader election service. This will not do any network
     * calls and is meant to be callable without major performance implications.
     *
     * @return the set of potential leaders known by this leader election service, including itself
     */
    Set<PingableLeader> getPotentialLeaders();

    /**
     * Attempts to give up leadership. Note that this does not guarantee that a different node will be elected the
     * leader - it is possible for this node to regain leadership without any other node acquiring it in the interim.
     *
     * @return true if and only if this node was able to cause itself to lose leadership
     */
    boolean stepDown();
}
