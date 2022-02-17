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

import com.palantir.atlasdb.AtlasDbConstants;

public final class OracleIdentifierLengthLimitOptions {
    // This sequencing of dependencies may look strange, but is necessary to avoid breaking back-compat with users of
    // the Oracle constants in AtlasDbConstants.
    static final OracleIdentifierLengthLimits LEGACY_PRE_ORACLE_12_2 =
            ImmutableOracleIdentifierLengthLimits.builder()
            .identifierLengthLimit(AtlasDbConstants.ORACLE_PRE_12_2_NAME_LENGTH_LIMIT)
            .tablePrefixLengthLimit(AtlasDbConstants.MAX_TABLE_PREFIX_LENGTH)
            .overflowTablePrefixLengthLimit(AtlasDbConstants.MAX_OVERFLOW_TABLE_PREFIX_LENGTH)
            .build();

    // 48 is a bit random, but should be adequate
    static final OracleIdentifierLengthLimits ORACLE_12_2 = ImmutableOracleIdentifierLengthLimits.builder()
            .identifierLengthLimit(AtlasDbConstants.ORACLE_12_2_NAME_LENGTH_LIMIT)
            .tablePrefixLengthLimit(48)
            .overflowTablePrefixLengthLimit(48)
            .build();

    private OracleIdentifierLengthLimitOptions() {
        // constants
    }
}
