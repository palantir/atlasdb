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
package com.palantir.atlasdb.keyvalue.cassandra.paging;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.KeySlice;

import com.google.common.collect.Maps;

public class DelegatingColumnGetter implements ColumnGetter {

    @Override
    public Map<ByteBuffer, List<ColumnOrSuperColumn>> getColumnsByRow(List<KeySlice> firstPage) {
        return getColsByKey(firstPage);
    }

    private Map<ByteBuffer, List<ColumnOrSuperColumn>> getColsByKey(List<KeySlice> firstPage) {
        Map<ByteBuffer, List<ColumnOrSuperColumn>> ret = Maps.newHashMapWithExpectedSize(firstPage.size());
        for (KeySlice e : firstPage) {
            ret.put(ByteBuffer.wrap(e.getKey()), e.getColumns());
        }
        return ret;
    }
}
