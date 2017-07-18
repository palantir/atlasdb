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

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.logsafe.Arg;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

/**
 * Includes utilities for generating logging args that may be safe or unsafe, depending on table metadata.
 *
 * Always returns unsafe, until hydrated.
 */
public final class LoggingArgs {
    private static volatile KeyValueServiceLogArbitrator logArbitrator = KeyValueServiceLogArbitrator.ALL_UNSAFE;

    private LoggingArgs() {
        // no
    }

    public static synchronized void hydrate(Map<TableReference, byte[]> tableRefToMetadata) {
        logArbitrator = SafeLoggableDataUtils.fromTableMetadata(tableRefToMetadata);
    }

    @VisibleForTesting
    static synchronized void setLogArbitrator(KeyValueServiceLogArbitrator arbitrator) {
        logArbitrator = arbitrator;
    }

    /**
     * Returns a safe or unsafe arg corresponding to the supplied table reference, with name "tableRef".
     */
    public static Arg<TableReference> tableRef(TableReference tableReference) {
        return tableRef("tableRef", tableReference);
    }

    public static Arg<TableReference> tableRef(String argName, TableReference tableReference) {
        return getArg(argName, tableReference, logArbitrator.isTableReferenceSafe(tableReference));
    }

    public static Arg<String> rowComponent(String argName, TableReference tableReference, String rowComponentName) {
        return getArg(argName,
                rowComponentName,
                logArbitrator.isRowComponentNameSafe(tableReference, rowComponentName));
    }

    public static Arg<String> columnName(String argName, TableReference tableReference, String columnName) {
        return getArg(argName,
                columnName,
                logArbitrator.isColumnNameSafe(tableReference, columnName));
    }

    private static <T> Arg<T> getArg(String name, T value, boolean safe) {
        return safe ? SafeArg.of(name, value) : UnsafeArg.of(name, value);
    }
}
