/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.config;

import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.palantir.atlasdb.AtlasDbConstants;

@Value.Immutable
public abstract class TimestampClientConfig {
    @Value.Default
    public boolean enableTimestampBatching() {
        return AtlasDbConstants.DEFAULT_ENABLE_TIMESTAMP_BATCHING;
    }

    @Value.Default
    public long getMinimumMillisBetweenBatches() {
        return AtlasDbConstants.DEFAULT_MINIMUM_MILLIS_BETWEEN_BATCHES;
    }

    @Value.Check
    protected final void check() {
        Preconditions.checkState(getMinimumMillisBetweenBatches() >= 0,
                "Minimum time interval between batches must be a positive integer, but found %s",
                getMinimumMillisBetweenBatches());
    }
}
