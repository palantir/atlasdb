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

public class QueryFormingTest {

    private TableReference dummyTableReference = TableReference.create(Namespace.DEFAULT_NAMESPACE, "test");

    private String dummyGetQuery = "SELECT value, column2 FROM test.default.test "
            + "WHERE key = :key AND column1 = :column1 AND column2 > :column2 ;";

    private QueryFormer dummyQueryFormer = new AbstractQueryFormer() {

        @Override
        public String formQuery(SupportedQuery supportedQuery, String keySpace, TableReference tableReference) {
            String normalizedName = AbstractQueryFormer.normalizeName(keySpace, tableReference);
            return String.format(QUERY_FORMATS_MAP.get(supportedQuery), normalizedName);
        }
    };


    @Test
    public void testCorrectNameNormalization() {
        String normalizedName = AbstractQueryFormer.normalizeName("test", dummyTableReference);
        assertThat(normalizedName).isEqualTo("test.default.test");
    }

    @Test
    public void testCorrectGetQueryForming() {
        String formedQuery = dummyQueryFormer.formQuery(QueryFormer.SupportedQuery.GET, "test",
                dummyTableReference);
        assertThat(formedQuery).isEqualTo(dummyGetQuery);
    }

    @Test
    public void testConsistentBehaviour() {
        String firstFormed = dummyQueryFormer.formQuery(QueryFormer.SupportedQuery.GET, "test",
                dummyTableReference);
        String secondFormed = dummyQueryFormer.formQuery(QueryFormer.SupportedQuery.GET, "test",
                dummyTableReference);
        assertThat(firstFormed).isEqualTo(secondFormed);
    }
}
