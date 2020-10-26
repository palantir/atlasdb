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

package com.palantir.timelock.history.models;

import com.palantir.paxos.PaxosAcceptor;
import com.palantir.timelock.history.sqlite.LogVerificationProgressState;
import org.immutables.value.Value;

@Value.Immutable
public interface ProgressState {
    @Value.Parameter
    long lastVerifiedSeq();

    @Value.Parameter
    long greatestSeqNumberToBeVerified();

    static ImmutableProgressState.Builder builder() {
        return ImmutableProgressState.builder();
    }

    default boolean shouldResetProgressState() {
        if (initStateOrInactiveClient() || lastVerifiedSeq() < greatestSeqNumberToBeVerified()) {
            return false;
        }
        return true;
    }

    default boolean initStateOrInactiveClient() {
        return lastVerifiedSeq() == LogVerificationProgressState.INITIAL_PROGRESS
                || greatestSeqNumberToBeVerified() == PaxosAcceptor.NO_LOG_ENTRY;
    }
}
