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
import java.net.URL;
import java.util.Optional;

import com.palantir.atlasdb.metrics.Timed;

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
     * Indicates that this node should no longer try to nominate itself for leadership in the context of
     * this leader election service.
     *
     * <b>Note</b> This only affects future elections, does not cause any change in leadership if this node is
     * currently leader. If this node has proposed leadership in an ongoing election,
     * that proposal still stands and the node could subsequently gain leadership.
     *
     * @see #stepDown()
     */
    void markNotEligibleForLeadership();

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
    @Timed
    Optional<LeadershipToken> getCurrentTokenIfLeading();

    /**
     * This method actually ensures that there is a quorum of supporters that this node is still leading.
     * If this node cannot get a quorum that it is still the leader then this method will return false and this node
     * may not respond to requests and must get back in line by calling {@link #blockOnBecomingLeader()}.
     *
     * @param token leadership token
     * @return LEADING if the token is still the leader
     */
    @Timed
    StillLeadingStatus isStillLeading(LeadershipToken token);

    /**
     * Attempts to give up leadership. Note that this does not guarantee that a different node will be elected the
     * leader - it is possible for this node to regain leadership without any other node acquiring it in the interim.
     *
     * @return true if and only if this node was able to cause itself to lose leadership
     */
    boolean stepDown();

    Optional<URL> getCurrentLeader();
}
