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
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class KeyValueServiceArgSupplier {
    public static final KeyValueServiceArgSupplier NO_OP = new KeyValueServiceArgSupplier();

    private final KeyValueServiceLogArbitrator logArbitrator;

    public KeyValueServiceArgSupplier(SafeLoggableData safeLoggableData) {
        this.logArbitrator = new SimpleKeyValueServiceLogArbitrator(safeLoggableData);
    }

    private KeyValueServiceArgSupplier() {
        // By default, nothing is safe.
        this.logArbitrator = new KeyValueServiceLogArbitrator() {
            @Override
            public boolean isTableReferenceSafe(TableReference tableReference) {
                return false;
            }

            @Override
            public boolean isRowComponentNameSafe(TableReference tableReference,
                    NameComponentDescription nameComponentDescription) {
                return false;
            }

            @Override
            public boolean isColumnNameSafe(TableReference tableReference,
                    NamedColumnDescription namedColumnDescription) {
                return false;
            }
        };
    }

    public <T> Arg<T> getArgDependingOnTableReference(String name, TableReference tableReference, T value) {
        return getArg(name, value, logArbitrator.isTableReferenceSafe(tableReference));
    }

    public <T> Arg<T> getArgDependingOnRowComponentName(String name, TableReference tableReference,
            NameComponentDescription nameComponentDescription, T value) {
        return getArg(
                name,
                value,
                logArbitrator.isRowComponentNameSafe(tableReference, nameComponentDescription));
    }

    public <T> Arg<T> getArgDependingOnColumnName(String name, TableReference tableReference,
            NamedColumnDescription namedColumnDescription, T value) {
        return getArg(
                name,
                value,
                logArbitrator.isColumnNameSafe(tableReference, namedColumnDescription));
    }

    private <T> Arg<T> getArg(String name, T value, boolean safe) {
        return safe ? SafeArg.of(name, value) : UnsafeArg.of(name, value);
    }
}
