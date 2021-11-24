/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.schema;

import com.google.common.collect.ImmutableSet;

public class TargetedSweepTables {
    private static final String PREFIX = "sweep__";
    static final String SWEEP_PROGRESS_PER_SHARD = "sweepProgressPerShard";
    static final String SWEEP_ID_TO_NAME = "sweepIdToName";
    static final String SWEEP_NAME_TO_ID = "sweepNameToId";
    static final String TABLE_CLEARS = "tableClears";

    static final String TARGETED_SWEEP_PROGRESS = PREFIX + SWEEP_PROGRESS_PER_SHARD;
    static final String TARGETED_SWEEP_ID_TO_NAME = PREFIX + SWEEP_ID_TO_NAME;
    static final String TARGETED_SWEEP_NAME_TO_ID = PREFIX + SWEEP_NAME_TO_ID;
    static final String TARGETED_SWEEP_TABLE_CLEARS = PREFIX + TABLE_CLEARS;

    public static final ImmutableSet<String> REPAIR_ON_RESTORE = ImmutableSet.of(
            TARGETED_SWEEP_PROGRESS, TARGETED_SWEEP_ID_TO_NAME, TARGETED_SWEEP_NAME_TO_ID, TARGETED_SWEEP_TABLE_CLEARS);
}
