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
package com.palantir.atlasdb.keyvalue.api;

import java.util.Collection;
import org.immutables.value.Value;

@Value.Immutable
public interface CandidateCellForSweeping {

    Cell cell();

    /**
     * All start timestamps for the cell that are strictly less than
     * {@link CandidateCellForSweepingRequest#maxTimestampExclusive()} and are not in
     * {@link CandidateCellForSweepingRequest#timestampsToIgnore()}, in ascending order.
     *
     * If the array is empty, then the cell is not an actual candidate and is only returned
     * for the purpose of reporting the number of examined cells.
     */
    Collection<Long> sortedTimestamps();

    /**
     * If {@link CandidateCellForSweepingRequest#shouldCheckIfLatestValueIsEmpty()} was set to true,
     * then this method returns true if and only if the value corresponding to the last entry of
     * {@link #sortedTimestamps()} is empty.
     * <p>
     * Otherwise, the return value is undefined and depends on the implementation.
     */
    boolean isLatestValueEmpty();
}
