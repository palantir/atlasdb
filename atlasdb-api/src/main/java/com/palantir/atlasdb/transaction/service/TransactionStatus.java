/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.service;

import org.derive4j.Data;

/**
 * Status of a transaction can be one of the following -
 * 1. IN_PROGRESS, if that the transaction with this start timestamp has started but has not committed yet,
 * 2. COMMITTED (with a commit timestamp), if the transaction with this start timestamp has been committed,
 * 3. ABORTED, if the transaction with this start timestamp has been aborted,
 * 4. UNKNOWN, if the transactions table does not have information about the start timestamp. (This may,
 *    for example, be because the transaction is known to have concluded and the transactions table has been swept).
 * */
@Data
public interface TransactionStatus {
    interface Cases<R> {
        /**
         * For transactions on schema version >= 4, an inProgress state has been introduced. The state will be set in
         * the transactions table against the start timestamp. The state will be written before writing to the kvs.
         *
         * Previously, absence of any state for a transaction would indicate that it is in progress.
         * */
        R inProgress();

        R committed(long commitTimestamp);

        R aborted();

        /**
         * From transaction schema version 4, we have added the ability to truncate the transactions table i.e. it
         * would be possible that the transactions table is not the source of truth for state of a transaction.
         *
         * Note that this state is not expected for transaction schema versions < 4.
         * */
        R unknown();
    }

    <R> R match(Cases<R> cases);

    @Override
    boolean equals(Object other);
}
