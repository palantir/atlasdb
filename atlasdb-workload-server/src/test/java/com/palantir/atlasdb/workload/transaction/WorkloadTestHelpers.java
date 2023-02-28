/*
 * (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.workload.transaction;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.workload.store.ImmutableWorkloadCell;
import com.palantir.atlasdb.workload.store.TableWorkloadCell;
import com.palantir.atlasdb.workload.store.WorkloadCell;

public final class WorkloadTestHelpers {
    public static final String TABLE = "foo";
    public static final TableReference TABLE_REFERENCE = TableReference.create(Namespace.DEFAULT_NAMESPACE, TABLE);

    public static final WorkloadCell WORKLOAD_CELL_1 = ImmutableWorkloadCell.of(1, 2);
    public static final WorkloadCell WORKLOAD_CELL_2 = ImmutableWorkloadCell.of(3, 4);
    public static final Integer VALUE_ONE = 5;
    public static final Integer VALUE_TWO = 3;
    public static final TableWorkloadCell TABLE_WORKLOAD_CELL_1 = TableWorkloadCell.of(TABLE, WORKLOAD_CELL_1);
    public static final TableWorkloadCell TABLE_WORKLOAD_CELL_2 = TableWorkloadCell.of(TABLE, WORKLOAD_CELL_2);
}
