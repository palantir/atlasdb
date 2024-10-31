/*
 * (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.impl;

import com.google.common.collect.Maps;
import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import com.palantir.atlasdb.transaction.api.TimestampLeaseAwareTransaction;
import com.palantir.atlasdb.transaction.api.TimestampLeaseAwareTransaction.PreCommitAction;
import com.palantir.atlasdb.transaction.api.Transaction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.immutables.value.Value;

/**
 * Keeps track of preCommit actions added through the course of a {@link Transaction}.
 * Such actions are fetched via {@link #getActions()} and executed on {@link Transaction#commit()}.
 * See {@link TimestampLeaseAwareTransaction#preCommit(TimestampLeaseName, int, PreCommitAction)} for more information.
 */
@ThreadSafe
final class TransactionPreCommitActions {

    @Value.Immutable
    interface PreCommitActionWrapper {
        @Value.Parameter
        PreCommitAction action();

        @Value.Parameter
        int numLeasedTimestamps();

        static PreCommitActionWrapper of(PreCommitAction action, int numLeasedTimestamps) {
            return ImmutablePreCommitActionWrapper.of(action, numLeasedTimestamps);
        }
    }

    static class PerLeaseActions {
        final List<PreCommitActionWrapper> preCommitActions;
        int numLeasedTimestamps;

        PerLeaseActions() {
            preCommitActions = new ArrayList<>();
            numLeasedTimestamps = 0;
        }

        private PerLeaseActions(List<PreCommitActionWrapper> actions, int numLeasedTimestamps) {
            preCommitActions = actions;
            this.numLeasedTimestamps = numLeasedTimestamps;
        }

        PerLeaseActions copy() {
            return new PerLeaseActions(new ArrayList<>(preCommitActions), numLeasedTimestamps);
        }
    }

    @GuardedBy("this")
    private final Map<TimestampLeaseName, PerLeaseActions> actions = new HashMap<>();

    synchronized void addPreCommitAction(
            TimestampLeaseName timestampLeaseName, int numLeasedTimestamps, PreCommitAction action) {
        PerLeaseActions perLeaseActions = actions.computeIfAbsent(timestampLeaseName, _unused -> new PerLeaseActions());
        perLeaseActions.numLeasedTimestamps += numLeasedTimestamps;
        perLeaseActions.preCommitActions.add(PreCommitActionWrapper.of(action, numLeasedTimestamps));
    }

    /**
     * A copy of all preCommit actions added through the course of the transaction.
     */
    synchronized Map<TimestampLeaseName, PerLeaseActions> getActions() {
        return new HashMap<>(Maps.transformValues(actions, PerLeaseActions::copy));
    }
}
