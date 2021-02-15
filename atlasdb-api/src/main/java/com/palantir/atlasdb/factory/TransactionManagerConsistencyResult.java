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
package com.palantir.atlasdb.factory;

import java.util.Optional;
import org.immutables.value.Value;

/**
 * A TransactionManagerConsistencyResult may be returned from a consistency check, which AtlasDB can execute to
 * determine that it is safe (or, at least, not obviously unsafe) to service requests.
 */
@Value.Immutable
@SuppressWarnings("ClassInitializationDeadlock")
public interface TransactionManagerConsistencyResult {
    @Deprecated
    TransactionManagerConsistencyResult CONSISTENT_RESULT = ImmutableTransactionManagerConsistencyResult.builder()
            .consistencyState(ConsistencyState.CONSISTENT)
            .build();

    ConsistencyState consistencyState();

    Optional<Throwable> reasonForInconsistency();

    enum ConsistencyState {
        /**
         * Indicates that the transaction manager is in a state that is inconsistent, and automated recovery
         * is generally not possible. Typically, to avoid further inconsistencies, AtlasDB will refuse to serve
         * requests in this state; manual action will be needed.
         */
        TERMINAL(2),
        /**
         * Indicates that we don't know whether the transaction manager is in a consistent state or not.
         */
        INDETERMINATE(1),
        /**
         * Indicates that we did not detect any inconsistencies in the transaction manager.
         */
        CONSISTENT(0);

        private final int severity;

        ConsistencyState(int severity) {
            this.severity = severity;
        }

        public int severity() {
            return severity;
        }
    }
}
