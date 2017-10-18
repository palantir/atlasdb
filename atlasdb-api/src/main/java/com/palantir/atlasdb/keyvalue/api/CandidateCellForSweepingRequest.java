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
     *  Only start timestamps that are strictly below this number will be considered.
     */
    long sweepTimestamp();

    /**
     *  In practice, this is true for the THOROUGH sweep strategy and false for CONSERVATIVE.
     */
    boolean shouldCheckIfLatestValueIsEmpty();

    /*
     *  In practice, this is the empty set if for the THOROUGH sweep strategy and { -1 } for CONSERVATIVE.
     */
    long[] timestampsToIgnore();

}
