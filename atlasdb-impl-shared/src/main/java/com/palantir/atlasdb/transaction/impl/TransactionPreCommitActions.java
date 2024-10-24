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

import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
final class TransactionPreCommitActions {

    static class PreCommitAction {
        final Consumer<LongSupplier> action;
        final int timestampCount;

        PreCommitAction(Consumer<LongSupplier> action, int timestampCount) {
            this.action = action;
            this.timestampCount = timestampCount;
        }
    }

    static class PerLeaseActions {
        final List<PreCommitAction> preCommitActions = new ArrayList<>();
        int timestampCount = 0;
    }

    @GuardedBy("this")
    private final Map<TimestampLeaseName, PerLeaseActions> actions = new HashMap<>();

    synchronized void addPreCommitAction(
            TimestampLeaseName timestampLeaseName, int numLeasedTimestamps, Consumer<LongSupplier> action) {
        PerLeaseActions perLeaseActions = actions.computeIfAbsent(timestampLeaseName, _unused -> new PerLeaseActions());
        perLeaseActions.timestampCount += numLeasedTimestamps;
        perLeaseActions.preCommitActions.add(new PreCommitAction(action, numLeasedTimestamps));
    }

    synchronized Map<TimestampLeaseName, PerLeaseActions> getActions() {
        return new HashMap<>(actions);
    }
}
