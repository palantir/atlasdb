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

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.Serializable;
import java.util.Optional;

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
     * This method will block until this node becomes the leader and is supported by a quorum of nodes. If for any
     * reason leadership cannot be obtained, this will throw a {@link InterruptedException}.
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
    ListenableFuture<StillLeadingStatus> isStillLeading(LeadershipToken token);

    /**
     * Attempts to give up leadership. Note that this does not guarantee that a different node will be elected the
     * leader - it is possible for this node to regain leadership without any other node acquiring it in the interim.
     *
     * @return true if and only if this node was able to cause itself to lose leadership
     */
    boolean stepDown();

    /**
     * Attempts to forcefully take over leadership. That is, it is similar to {@link #blockOnBecomingLeader()}, however
     * it does not block and also if it discovers that it is not the leader, it doesn't back down but proposes
     * leadership anyway.
     * <p>
     * Note: Whilst a positive result means that it successfully proposed and became the leader, it is still subject to
     * health check constraints, that is, if it becomes unresponsive for any reason, other nodes will take over
     * leadership through the normal means via {@link #blockOnBecomingLeader}.
     *
     * @return true if and only if this node was able to gain leadership forcefully or was already the leader
     */
    boolean hostileTakeover();

    /**
     * If this {@link LeaderElectionService} has last successfully pinged node that believed it was the leader recently
     * (up to the implementation), it returns the {@link HostAndPort} through which the leader can be contacted.
     * <p>
     * In practice, there may be a few false positives, but assuming everything is working as intended, they should
     * quickly resolve.
     * <p>
     * @return {@link Optional} containing address of suspected leader, otherwise empty if this
     * {@link LeaderElectionService} has not been able to contact the leader recently.
     */
    Optional<HostAndPort> getRecentlyPingedLeaderHost();
}
