/*
 * (c) Copyright 2019 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.keyvalue.cassandra.async.query.forming;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.tritium.metrics.registry.SharedTaggedMetricRegistries;

public class QueryFormingTest {

    private static final TableReference DUMMY_TABLE_REFERENCE = TableReference.create(Namespace.DEFAULT_NAMESPACE,
            "test");

    private static final String DUMMY_GET_QUERY = "SELECT value, column2 FROM test.default__test "
            + "WHERE key = :row AND column1 = :column AND column2 > :timestamp;";

    private static final QueryFormer DUMMY_QUERY_FORMER = CacheQueryFormer
            .create(SharedTaggedMetricRegistries.getSingleton(), 100);


    @Test
    public void testCorrectGetQueryForming() {
        String formedQuery = DUMMY_QUERY_FORMER.formQuery(SupportedQuery.GET, "test",
                DUMMY_TABLE_REFERENCE);

        assertThat(formedQuery).isEqualTo(DUMMY_GET_QUERY);
    }
}
