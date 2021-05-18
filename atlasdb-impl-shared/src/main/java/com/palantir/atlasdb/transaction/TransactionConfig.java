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
package com.palantir.atlasdb.transaction;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.transaction.impl.TransactionRetryStrategy;
import com.palantir.conjure.java.api.config.service.HumanReadableDuration;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableTransactionConfig.class)
@JsonSerialize(as = ImmutableTransactionConfig.class)
@Value.Immutable
public abstract class TransactionConfig {

    @Value.Default
    public long getLockAcquireTimeoutMillis() {
        return AtlasDbConstants.DEFAULT_TRANSACTION_LOCK_ACQUIRE_TIMEOUT_MS;
    }

    @Value.Default
    public int getThresholdForLoggingLargeNumberOfTransactionLookups() {
        return AtlasDbConstants.THRESHOLD_FOR_LOGGING_LARGE_NUMBER_OF_TRANSACTION_LOOKUPS;
    }

    @Value.Default
    public TransactionRetryStrategy retryStrategy() {
        return TransactionRetryStrategy.Strategies.LEGACY.get();
    }

    /**
     * This value is ignored if {@link com.palantir.atlasdb.transaction.api.TransactionManager} is configured to lock
     * immutable ts by using TransactionManagers builder option.
     */
    @Value.Default
    public boolean lockImmutableTsOnReadOnlyTransactions() {
        return false;
    }

    /**
     * Indicates how long user transactions are allowed to take to commit, in terms of how long we'll refresh the
     * commit locks for. Note that locks may still require more time before TimeLock realises that they are no longer
     * actively held and releases them.
     */
    @Value.Default
    public HumanReadableDuration commitLockTenure() {
        return HumanReadableDuration.minutes(5);
    }

    /**
     * TODO(fdesouza): Remove this once PDS-95791 is resolved.
     * @deprecated Remove this once PDS-95791 is resolved.
     */
    @Deprecated
    @JsonProperty("do-not-use-attach-start-timestamp-to-locks-request")
    @Value.Default
    public boolean attachStartTimestampToLockRequestDescriptions() {
        return false;
    }
}
