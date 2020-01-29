/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.timelock.evaluation;

import java.util.function.BooleanSupplier;
import java.util.function.DoubleSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

public class PaxosStatusCheckRateInvariant implements Invariant {
    private static final Logger log = LoggerFactory.getLogger(PaxosStatusCheckRateInvariant.class);

    private final DoubleSupplier paxosStatusCheckOutboundRate;
    private final DoubleSupplier getFreshTimestampsResponseRate;
    private final BooleanSupplier believesSelfToBeLeader;

    public PaxosStatusCheckRateInvariant(
            DoubleSupplier paxosStatusCheckOutboundRate,
            DoubleSupplier getFreshTimestampsResponseRate,
            BooleanSupplier believesSelfToBeLeader) {
        this.paxosStatusCheckOutboundRate = paxosStatusCheckOutboundRate;
        this.getFreshTimestampsResponseRate = getFreshTimestampsResponseRate;
        this.believesSelfToBeLeader = believesSelfToBeLeader;
    }

    @Override
    public InvariantCheckState runOneIteration() {
        if (!believesSelfToBeLeader.getAsBoolean()) {
            log.debug("Paxos state check invariant not evaluated, because I am not the leader right now");
            return InvariantCheckState.noKnownViolation();
        }

        double paxosStatusCheckRate = paxosStatusCheckOutboundRate.getAsDouble();
        double getFreshTimestampsRate = getFreshTimestampsResponseRate.getAsDouble();

        if (paxosStatusCheckRate <= 0.0 && getFreshTimestampsRate > 0.0) {
            log.error("Paxos status check invariant failed! Status check rate was {}, getFreshTimestamps rate was {}",
                    SafeArg.of("paxosStatusCheckRate", paxosStatusCheckRate),
                    SafeArg.of("getFreshTimestampsRate", getFreshTimestampsRate));
            return InvariantCheckState.violation(
                    new SafeIllegalStateException(
                            "TimeLock is servicing fresh timestamps calls, but no Paxos outbound checks!"
                                    + " This is unexpected as every getFreshTimestamps call should require this.",
                            SafeArg.of("paxosStatusCheckRate", paxosStatusCheckRate),
                            SafeArg.of("getFreshTimestampsRate", getFreshTimestampsRate)));
        }

        log.debug("Paxos status check invariant passed: status check rate was {}, and getFreshTimestamps rate was {}",
                SafeArg.of("paxosStatusCheckRate", paxosStatusCheckRate),
                SafeArg.of("getFreshTimestampsRate", getFreshTimestampsRate));
        return InvariantCheckState.noKnownViolation();
    }
}
