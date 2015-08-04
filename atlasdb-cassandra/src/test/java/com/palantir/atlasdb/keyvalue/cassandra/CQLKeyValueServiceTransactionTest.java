/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.keyvalue.cassandra;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;

public class CQLKeyValueServiceTransactionTest extends AbstractTransactionTest {

    @Override
    protected KeyValueService getKeyValueService() {
        return CQLKeyValueService.create(
                ImmutableSet.of("localhost"),
                9160,
                20,
                1000,
                "atlasdb",
                false,
                1,
                1000,
                10000000,
                1000,
                true,
                false,
                null);
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

}
