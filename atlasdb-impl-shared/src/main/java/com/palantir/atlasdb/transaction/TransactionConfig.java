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

package com.palantir.atlasdb.transaction;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.AtlasDbConstants;

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
}
