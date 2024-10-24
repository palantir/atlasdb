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

package com.palantir.atlasdb.transaction.api;

import com.google.common.annotations.Beta;
import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

@Beta
public interface TimestampLeaseAwareTransaction {
    /**
     * Register pre-commit lambdas that are run on commit() just before the transaction is closed.
     * <p>
     * In the pre-commit lambdas, consumers can perform any actions they would inside a transaction.
     * It is valid, for example, to read to or write from a table.
     * <p>
     * If {@code numLeasedTimestamps} is greater than 0, fresh timestamps will be fetched from {@link TransactionManager#getTimelockService()}
     * and will be provided to the pre-commit lambda via the supplier on {@code Consumer<LongSupplier>}.
     * <p>
     * Clients can use {@link TransactionManager#getLeasedTimestamp(TimestampLeaseName)} to fetch a timestamp before the earliest
     * leased timestamp for a given {@code timestampLeaseName} on open transactions.
     *
     * @param leaseName the name of the lease the timestamps are bound to
     * @param numLeasedTimestamps the number of timestamps that should be fetched and used in the pre-commit lambda
     * @param preCommitAction the lambda executed just before commit. Note the consumer throws {@code RuntimeException}
     * if a client requests more than specified in {@code numLeasedTimestamps}.
     */
    void preCommit(TimestampLeaseName leaseName, int numLeasedTimestamps, Consumer<LongSupplier> preCommitAction);
}
