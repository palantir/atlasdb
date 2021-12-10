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
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.schema.generated.TargetedSweepTableFactory;

public class TargetedSweepTables {
    static final Namespace NAMESPACE = Namespace.create("sweep");

    private static final TargetedSweepTableFactory TABLE_FACTORY = TargetedSweepTableFactory.of();

    private static final TableReference SWEEP_PROGRESS_PER_SHARD_TABLE =
            TABLE_FACTORY.getSweepShardProgressTable(null).getTableRef();
    private static final TableReference SWEEP_ID_TO_NAME_TABLE =
            TABLE_FACTORY.getSweepIdToNameTable(null).getTableRef();
    private static final TableReference SWEEP_NAME_TO_ID_TABLE =
            TABLE_FACTORY.getSweepNameToIdTable(null).getTableRef();
    private static final TableReference TABLE_CLEARS_TABLE =
            TABLE_FACTORY.getTableClearsTable(null).getTableRef();

    public static final ImmutableSet<TableReference> REPAIR_ON_RESTORE = ImmutableSet.of(
            SWEEP_PROGRESS_PER_SHARD_TABLE, SWEEP_ID_TO_NAME_TABLE, SWEEP_NAME_TO_ID_TABLE, TABLE_CLEARS_TABLE);
}
