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
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class KeyValueServiceArgSupplier {
    public static final KeyValueServiceArgSupplier ALL_UNSAFE = new KeyValueServiceArgSupplier(
            KeyValueServiceLogArbitrator.ALL_UNSAFE);

    private final KeyValueServiceLogArbitrator logArbitrator;

    public KeyValueServiceArgSupplier(KeyValueServiceLogArbitrator logArbitrator) {
        this.logArbitrator = logArbitrator;
    }

    public Arg<TableReference> getTableReferenceArg(String argName, TableReference tableReference) {
        return getArg(argName, tableReference, logArbitrator.isTableReferenceSafe(tableReference));
    }

    public Arg<String> getRowComponentNameArg(String argName, TableReference tableReference, String rowComponentName) {
        return getArg(argName,
                rowComponentName,
                logArbitrator.isRowComponentNameSafe(tableReference, rowComponentName));
    }

    public Arg<String> getColumnNameArg(String argName, TableReference tableReference, String columnName) {
        return getArg(argName,
                columnName,
                logArbitrator.isColumnNameSafe(tableReference, columnName));
    }

    private <T> Arg<T> getArg(String name, T value, boolean safe) {
        return safe ? SafeArg.of(name, value) : UnsafeArg.of(name, value);
    }
}
