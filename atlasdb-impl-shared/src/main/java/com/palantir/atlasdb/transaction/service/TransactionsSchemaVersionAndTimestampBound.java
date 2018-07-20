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

package com.palantir.atlasdb.transaction.service;

import java.util.Optional;

import org.immutables.value.Value;

import com.google.common.base.Preconditions;

@Value.Immutable
public interface TransactionsSchemaVersionAndTimestampBound {
    /**
     * Schema version of the transactions table that should be used.
     *
     * In schema version 1, users write to the _transactions table.
     * In schema version 2, users split their writes between _transactions and _transactions2, depending on the value
     * of the transactions2LowerBound.
     */
    long transactionsSchemaVersion();

    /**
     * Validity is intended to be used as a construct in live migrations. Write transactions that start after the
     * specified timestamp may not be committed without first refreshing the validity of this bound, if live migrations
     * are being used.
     *
     * A value of -1 means that live migrations are not being used, and this value should be interpreted as correct.
     * Implementations supporting live migrations will set a bounded validity before accepting the information here
     * as correct.
     */
    @Value.Default
    default long validity() {
        return -1;
    }

    /**
     * If the schema version is 2, then:
     *   - this bound must be present
     *   - all transactions with start timestamps beneath this bound should be looked up in _transactions (1)
     *   - all transactions with start timestamps at or above this bound should be looked up in _transactions2
     *
     * No guarantees on this bound are made if the schema version is not equal to 2.
     */
    Optional<Long> transactions2LowerBound();

    @Value.Check
    default void checkBoundPresentIfRequired() {
        Preconditions.checkState(transactionsSchemaVersion() != 2 || transactions2LowerBound().isPresent(),
                "If the schema version is at least 2, the transactions2 lower bound must be present.");
    }
}
