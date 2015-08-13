/**
 * Copyright 2015 Palantir Technologies
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

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.AbstractSerializableTransactionTest;

public class CassandraKeyValueServiceSerializableTransactionTest extends
        AbstractSerializableTransactionTest {

    @Override
    protected KeyValueService getKeyValueService() {
        return CassandraKeyValueService.create(
                ImmutableSet.of("localhost"),
                9160,
                20,
                "atlasdb", false,
                1,
                10000,
                10000000,
                1000,
                false,
                false);
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

}
