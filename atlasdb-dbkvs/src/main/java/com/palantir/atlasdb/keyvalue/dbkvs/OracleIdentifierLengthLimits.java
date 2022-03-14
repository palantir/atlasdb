/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.dbkvs;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import org.immutables.value.Value;

/**
 * Tracks length limits on tables and identifier names within Oracle.
 */
@Value.Immutable
@JsonDeserialize(as = ImmutableOracleIdentifierLengthLimits.class)
@JsonSerialize(as = ImmutableOracleIdentifierLengthLimits.class)
public interface OracleIdentifierLengthLimits {
    int identifierLengthLimit();

    int tablePrefixLengthLimit();

    int overflowTablePrefixLengthLimit();

    @Value.Derived
    default int tableNameLengthLimit() {
        return identifierLengthLimit() - AtlasDbConstants.PRIMARY_KEY_CONSTRAINT_PREFIX.length();
    }

    @Value.Check
    default void check() {
        Preconditions.checkState(
                tablePrefixLengthLimit() < tableNameLengthLimit(),
                "Table prefix length limit must be shorter than the table name length limit",
                SafeArg.of("tablePrefixLengthLimit", tablePrefixLengthLimit()),
                SafeArg.of("tableNameLengthLimit", tableNameLengthLimit()));
        Preconditions.checkState(
                overflowTablePrefixLengthLimit() < tableNameLengthLimit(),
                "Overflow table prefix length limit must be shorter than the table name length limit",
                SafeArg.of("overflowTablePrefixLengthLimit", overflowTablePrefixLengthLimit()),
                SafeArg.of("tableNameLengthLimit", tableNameLengthLimit()));
    }
}
