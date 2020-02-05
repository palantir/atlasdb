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

import javax.annotation.concurrent.GuardedBy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.timelock.paxos.db.PaxosAtomicValue;
import com.palantir.common.remoting.ServiceNotAvailableException;
import com.palantir.leader.NotCurrentLeaderException;
import com.palantir.logsafe.SafeArg;
import com.palantir.paxos.PaxosAcceptorNetworkClient;
import com.palantir.paxos.PaxosLearner;
import com.palantir.paxos.PaxosLearnerNetworkClient;
import com.palantir.paxos.PaxosProposer;
import com.palantir.timestamp.DebugLogger;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public class PaxosTimestampBoundStore implements TimestampBoundStore {
    private static final Logger log = LoggerFactory.getLogger(PaxosTimestampBoundStore.class);

    private final PaxosAtomicValue<Long> atomicValue;

    @GuardedBy("this")
    private long currentBound;

    public PaxosTimestampBoundStore(
            PaxosProposer proposer,
            PaxosLearner knowledge,
            PaxosAcceptorNetworkClient acceptorNetworkClient,
            PaxosLearnerNetworkClient learnerClient,
            long maximumWaitBeforeProposalMs) {
        DebugLogger.logger.info("Creating PaxosTimestampBoundStore. The UUID of my proposer is {}. "
                        + "Currently, I believe the timestamp bound is {}.",
                SafeArg.of("proposerUuid", proposer.getUuid()),
                SafeArg.of("timestampBound", knowledge.getGreatestLearnedValue()));
        atomicValue = new PaxosAtomicValue<>(
                proposer,
                acceptorNetworkClient,
                knowledge,
                learnerClient,
                maximumWaitBeforeProposalMs,
                PtBytes::toBytes,
                PtBytes::toLong,
                0L);
    }

    /**
     * Contacts a quorum of nodes to find the latest sequence number prepared or accepted from acceptors,
     * and the bound associated with this sequence number. This method MUST be called at least once before
     * storeUpperLimit() is called for the first time.
     *
     * @return the upper limit the cluster has agreed on
     * @throws ServiceNotAvailableException if we couldn't contact a quorum
     */
    @Override
    public synchronized long getUpperLimit() {
        currentBound = atomicValue.get();
        return currentBound;
    }

    /**
     * Proposes a new timestamp limit, with sequence number 1 greater than the current agreed bound, or
     * PaxosAcceptor.NO_LOG_ENTRY + 1 if nothing has been proposed or accepted yet.
     *
     * @param limit the new upper limit to be stored
     * @throws IllegalArgumentException if trying to persist a limit smaller than the agreed limit
     * @throws NotCurrentLeaderException if the timestamp limit has changed out from under us
     */
    @Override
    public synchronized void storeUpperLimit(long limit) throws MultipleRunningTimestampServiceError {
        if (!atomicValue.compareAndSet(currentBound, limit)) {
            log.warn("It appears we updated the timestamp limit to {}, which was less than our target {}."
                            + " This suggests we have another timestamp service running; possibly because we"
                            + " lost and regained leadership. For safety, we are now stopping this service.",
                    SafeArg.of("newLimit", limit),
                    SafeArg.of("target", limit));
            throw new NotCurrentLeaderException(String.format(
                    "We updated the timestamp limit to %s, which was less than our target %s.",
                    limit));

        }
    }
}
