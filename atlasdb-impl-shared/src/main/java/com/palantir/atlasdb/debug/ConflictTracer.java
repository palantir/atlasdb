/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.debug;

import com.palantir.atlasdb.keyvalue.api.Cell;
import java.util.Map;

/**
 * TODO(fdesouza): Remove this once PDS-95791 is resolved.
 * @deprecated Remove this once PDS-95791 is resolved.
 */
@Deprecated
public interface ConflictTracer {
    ConflictTracer NO_OP = (startTimestamp, keysToLoad, latestTimestamps, commitTimestamps) -> { };

    void collect(
            long startTimestamp,
            Map<Cell, Long> keysToLoad,
            Map<Cell, Long> latestTimestamps,
            Map<Long, Long> commitTimestamps);
}
