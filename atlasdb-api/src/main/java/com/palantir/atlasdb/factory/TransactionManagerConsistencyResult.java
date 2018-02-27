/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.factory;

import java.util.Optional;

import org.immutables.value.Value;

@Value.Immutable
public interface TransactionManagerConsistencyResult {
    TransactionManagerConsistencyResult CONSISTENT_RESULT = ImmutableTransactionManagerConsistencyResult.builder()
            .consistencyState(ConsistencyState.CONSISTENT)
            .build();

    ConsistencyState consistencyState();

    Optional<Throwable> reasonForInconsistency();

    enum ConsistencyState {
        TERMINAL(false, 1_000_000),
        INDETERMINATE(false, 1_000),
        CONSISTENT(true, 0);

        private final boolean canServeRequests;
        private final int severity;

        ConsistencyState(boolean canServeRequests, int severity) {
            this.canServeRequests = canServeRequests;
            this.severity = severity;
        }

        public boolean canServeRequests() {
            return canServeRequests;
        }

        public int severity() {
            return severity;
        }
    }
}
