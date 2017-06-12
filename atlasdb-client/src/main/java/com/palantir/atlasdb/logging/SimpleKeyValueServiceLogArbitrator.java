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

package com.palantir.atlasdb.logging;

import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;

public class SimpleKeyValueServiceLogArbitrator implements KeyValueServiceLogArbitrator {
    private final SafeLoggableData safeLoggableData;

    public SimpleKeyValueServiceLogArbitrator(SafeLoggableData safeLoggableData) {
        this.safeLoggableData = safeLoggableData;
    }

    @Override
    public boolean isTableReferenceSafe(TableReference tableReference) {
        return safeLoggableData.permittedTableReferences().contains(tableReference);
    }

    @Override
    public boolean isRowComponentNameSafe(TableReference tableReference,
            NameComponentDescription nameComponentDescription) {
        return safeLoggableData.permittedRowComponents().get(tableReference).contains(nameComponentDescription);
    }

    @Override
    public boolean isColumnNameSafe(TableReference tableReference, NamedColumnDescription namedColumnDescription) {
        return safeLoggableData.permittedColumnNames().get(tableReference).contains(namedColumnDescription);
    }
}
