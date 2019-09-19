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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import java.util.Set;

public final class HiddenTables {
    private HiddenTables() {
        // utility
    }

    private static final Set<TableReference> CASSANDRA_TABLES = ImmutableSet.of(
            AtlasDbConstants.TIMESTAMP_TABLE,
            AtlasDbConstants.DEFAULT_METADATA_TABLE,
            AtlasDbConstants.PERSISTED_LOCKS_TABLE);

    public static boolean isHidden(TableReference tableReference) {
        return CASSANDRA_TABLES.contains(tableReference)
                || AtlasDbConstants.DEPRECATED_SWEEP_TABLES_WITH_NO_METADATA.contains(tableReference)
                || tableReference.getTablename().startsWith(AtlasDbConstants.LOCK_TABLE_PREFIX);
    }
}
