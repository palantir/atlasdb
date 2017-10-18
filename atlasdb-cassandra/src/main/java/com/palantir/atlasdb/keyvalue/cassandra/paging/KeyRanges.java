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

package com.palantir.atlasdb.keyvalue.cassandra.paging;

import org.apache.cassandra.thrift.KeyRange;

import com.palantir.atlasdb.keyvalue.api.RangeRequests;

public final class KeyRanges {
    private KeyRanges() {}

    public static KeyRange createKeyRange(byte[] startKey, byte[] endExclusive, int limit) {
        KeyRange keyRange = new KeyRange(limit);
        keyRange.setStart_key(startKey);
        if (endExclusive.length == 0) {
            keyRange.setEnd_key(endExclusive);
        } else {
            // We need the previous name because this is inclusive, not exclusive
            keyRange.setEnd_key(RangeRequests.previousLexicographicName(endExclusive));
        }
        return keyRange;
    }

}
