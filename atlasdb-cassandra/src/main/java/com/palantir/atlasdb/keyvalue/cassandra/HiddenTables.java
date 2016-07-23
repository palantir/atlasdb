/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.keyvalue.api.TableReference;

class HiddenTables {
    private TableReference lockTable;
    private final Set<TableReference> hiddenTables;
    public static final String LOCK_TABLE_PREFIX = "_locks";


    HiddenTables() {
        this.hiddenTables = ImmutableSet.of(
                AtlasDbConstants.TIMESTAMP_TABLE,
                AtlasDbConstants.METADATA_TABLE);
    }

    boolean isHidden(TableReference tableReference) {
        return hiddenTables.contains(tableReference) || tableReference.getTablename().startsWith(LOCK_TABLE_PREFIX);
    }
}
