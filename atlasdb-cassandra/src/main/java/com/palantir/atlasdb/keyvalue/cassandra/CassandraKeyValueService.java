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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.processors.AutoDelegate;
import java.util.List;

@AutoDelegate
public interface CassandraKeyValueService extends KeyValueService {
    CassandraTables getCassandraTables();
    TracingQueryRunner getTracingQueryRunner();
    CassandraClientPool getClientPool();
    @Override boolean isInitialized();
    /**
     * Returns a sorted list of row keys in the specified range.
     *
     * @param tableRef table for which the request is made.
     * @param startRow inclusive start of the row key range. Use empty byte array for unbounded.
     * @param endRow inclusive end of the row key range. Use empty byte array for unbounded.
     * @param maxResults the request only returns the first maxResults rows in range.
     */
    List<byte[]> getRowKeysInRange(TableReference tableRef, byte[] startRow, byte[] endRow, int maxResults);
}
