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

import com.google.errorprone.annotations.RestrictedApi;
import com.palantir.atlasdb.common.api.annotations.ReviewedRestrictedApiUsage;
import com.palantir.atlasdb.common.api.timelock.TimestampLeaseName;
import com.palantir.lock.v2.TimelockService;
import java.util.function.Consumer;

public interface TimestampLeaseAwareTransactionManager {
    /**
     * Returns the timestamp that is before leased timestamps returned by the consumer on {@link TimestampLeaseAwareTransaction#preCommit(TimestampLeaseName, int, Consumer)}
     * for a {@code timestampLeaseName} in open transactions.
     * <p>
     * This is similar to {@link TransactionManager#getImmutableTimestamp()} as it returns a timestamp before timestamps
     * in open transactions, but for the immutable timestamp the timestamps considered are start timestamps for open
     * transactions, while for leased timestamps the timestamps considered are leased timestamps from the corresponding
     * {@code timestampLeaseName} in open transactions.
     * <p>
     * If no transactions with a {@code timestampLeaseName} lock are open, this method returns a new fresh timestamp
     * (i.e. equivalent to {@link TimelockService#getFreshTimestamp()}).
     * <p>
     * Consumers should to fetch the leased timestamp outside of transactions that potentially use it - if fetching the
     * leased timestamp inside a transaction, it's possible for the transaction's start timestamp < leased timestamp,
     * meaning the transaction cannot read all data up to leased timestamp.
     *
     * @param leaseName the name of the lease the timestamps are bound to
     * @return the timestamp that is before any timestamp returned by the consumer of {@link TimestampLeaseAwareTransaction#preCommit(TimestampLeaseName, int, Consumer)}
     * for open transactions.
     */
    @RestrictedApi(
            explanation = "This API is only meant to be used by AtlasDb proxies that want to make use of the"
                    + " performance improvements by tracking leased timestamps timestamps of open transactions."
                    + " Misuse of this feature can cause correctness issues.",
            link = "https://github.com/palantir/atlasdb/pull/7305",
            allowedOnPath = ".*/src/test/.*", // Unsafe behavior in tests is ok.
            allowlistAnnotations = {ReviewedRestrictedApiUsage.class})
    long getLeasedTimestamp(TimestampLeaseName leaseName);
}
