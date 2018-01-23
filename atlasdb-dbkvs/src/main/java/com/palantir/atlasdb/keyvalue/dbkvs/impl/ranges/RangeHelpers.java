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
package com.palantir.atlasdb.keyvalue.dbkvs.impl.ranges;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.RangeRequest;

public final class RangeHelpers {
    private RangeHelpers() { }

    public static int getMaxRowsPerPage(RangeRequest rangeRequest) {
        if (rangeRequest.getBatchHint() != null) {
            return Math.max(1, rangeRequest.getBatchHint());
        } else {
            return 100;
        }
    }

    public static <T> ImmutableSortedMap.Builder<byte[], T> newColumnMap() {
        return ImmutableSortedMap.orderedBy(UnsignedBytes.lexicographicalComparator());
    }

}
