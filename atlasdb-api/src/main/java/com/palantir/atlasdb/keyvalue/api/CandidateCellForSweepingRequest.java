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
package com.palantir.atlasdb.keyvalue.api;

import java.util.OptionalInt;

import org.immutables.value.Value;

@Value.Immutable
public interface CandidateCellForSweepingRequest {
    byte[] startRowInclusive();

    OptionalInt batchSizeHint();

    /**
     *  The maximum timestamp to be returned in the resulting {@link CandidateCellForSweeping} objects.
     */
    long maxTimestampExclusive();

    /**
     *  In practice, this is true for the THOROUGH sweep strategy and false for CONSERVATIVE.
     */
    boolean shouldCheckIfLatestValueIsEmpty();

    /*
     *  Whether GC sentinels (values written at timestamp -1) should be swept. In practice, this is true for the
     *  THOROUGH sweep strategy and false for CONSERVATIVE.
     */
    boolean ignoreGarbageCollectionSentinels();

    default boolean shouldSweep(long timestamp) {
        if (ignoreGarbageCollectionSentinels()
                && timestamp == com.palantir.atlasdb.keyvalue.api.Value.INVALID_VALUE_TIMESTAMP) {
            return false;
        }

        return timestamp < maxTimestampExclusive();
    }

    default CandidateCellForSweepingRequest withStartRow(byte[] startRow) {
        return ImmutableCandidateCellForSweepingRequest.builder()
                .from(this)
                .startRowInclusive(startRow)
                .build();
    }

}
