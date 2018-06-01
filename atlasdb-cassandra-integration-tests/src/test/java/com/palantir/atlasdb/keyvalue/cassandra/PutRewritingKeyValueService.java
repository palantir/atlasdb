/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyAlreadyExistsException;
import com.palantir.atlasdb.keyvalue.api.TableReference;

public class PutRewritingKeyValueService implements AutoDelegate_CassandraKeyValueService {
    private final CassandraKeyValueService delegate;

    public PutRewritingKeyValueService(CassandraKeyValueService delegate) {
        this.delegate = delegate;
    }

    @Override
    public CassandraKeyValueService delegate() {
        return delegate;
    }

    @Override
    public void put(TableReference arg0, Map<Cell, byte[]> arg1, long arg2) throws KeyAlreadyExistsException {
        multiPut(ImmutableMap.of(arg0, arg1), arg2);
    }
}
