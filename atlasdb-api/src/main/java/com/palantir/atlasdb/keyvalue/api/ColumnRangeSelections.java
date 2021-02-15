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
package com.palantir.atlasdb.keyvalue.api;

import com.palantir.common.persist.Persistable;
import com.palantir.logsafe.Preconditions;
import java.io.Serializable;

public final class ColumnRangeSelections implements Serializable {
    private static final long serialVersionUID = 1L;

    private ColumnRangeSelections() {
        // Utility class
    }

    public static BatchColumnRangeSelection createPrefixRange(byte[] prefix, int batchSize) {
        byte[] startCol =
                Preconditions.checkNotNull(prefix, "prefix cannot be null").clone();
        byte[] endCol = RangeRequests.createEndNameForPrefixScan(prefix);
        return BatchColumnRangeSelection.create(startCol, endCol, batchSize);
    }

    public static BatchColumnRangeSelection createPrefixRange(Persistable persistable, int batchSize) {
        return createPrefixRange(persistable.persistToBytes(), batchSize);
    }
}
