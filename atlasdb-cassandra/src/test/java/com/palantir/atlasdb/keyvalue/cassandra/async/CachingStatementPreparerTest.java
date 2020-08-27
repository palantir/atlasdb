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

package com.palantir.atlasdb.keyvalue.cassandra.async;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.driver.core.PreparedStatement;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.GetQuerySpec;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.GetQuerySpec.GetQueryParameters;
import com.palantir.atlasdb.keyvalue.cassandra.async.queries.ImmutableCqlQueryContext;
import com.palantir.atlasdb.keyvalue.cassandra.async.statement.preparing.CachingStatementPreparer;
import com.palantir.atlasdb.keyvalue.cassandra.async.statement.preparing.StatementPreparer;
import com.palantir.atlasdb.util.MetricsManager;
import com.palantir.atlasdb.util.MetricsManagers;
import org.junit.Before;
import org.junit.Test;

public class CachingStatementPreparerTest {

    private static final StatementPreparer STATEMENT_PREPARER =
            querySpec -> mock(PreparedStatement.class);
    private static final MetricsManager METRICS_MANAGER = MetricsManagers.createForTests();
    private static final String KEYSPACE = "foo";
    private static final TableReference TABLE_REFERENCE = tableReference("bar");
    private static final GetQueryParameters GET_QUERY_PARAMETERS = mock(GetQueryParameters.class);

    private CachingStatementPreparer cache;

    @Before
    public void setUp() {
        cache = CachingStatementPreparer.create(STATEMENT_PREPARER, METRICS_MANAGER.getTaggedRegistry(), 100);
    }

    @Test
    public void testGetQuerySpecAllSame() {
        PreparedStatement expectedPreparedStatement = prepareStatement(KEYSPACE, TABLE_REFERENCE, GET_QUERY_PARAMETERS);
        assertThat(prepareStatement(KEYSPACE, TABLE_REFERENCE, GET_QUERY_PARAMETERS))
                .isEqualTo(expectedPreparedStatement);
    }

    @Test
    public void testGetQuerySpecParametersIgnored() {
        PreparedStatement expectedPreparedStatement = prepareStatement(KEYSPACE, TABLE_REFERENCE, GET_QUERY_PARAMETERS);
        assertThat(prepareStatement(KEYSPACE, TABLE_REFERENCE, mock(GetQueryParameters.class)))
                .isEqualTo(expectedPreparedStatement);
    }

    @Test
    public void testGetQuerySpecKeyspaceNotIgnored() {
        PreparedStatement expectedPreparedStatement = prepareStatement(KEYSPACE, TABLE_REFERENCE, GET_QUERY_PARAMETERS);
        assertThat(prepareStatement(randomAlphabetic(5), TABLE_REFERENCE, GET_QUERY_PARAMETERS))
                .isNotEqualTo(expectedPreparedStatement);
    }

    @Test
    public void testGetQuerySpecTableRefNotIgnored() {
        PreparedStatement expectedPreparedStatement = prepareStatement(KEYSPACE, TABLE_REFERENCE, GET_QUERY_PARAMETERS);
        assertThat(prepareStatement(KEYSPACE, tableReference(randomAlphabetic(5)), GET_QUERY_PARAMETERS))
                .isNotEqualTo(expectedPreparedStatement);
    }

    private PreparedStatement prepareStatement(
            String keyspace,
            TableReference tableReference,
            GetQueryParameters getQueryParameters) {
        return cache.prepare(getQuerySpec(keyspace, tableReference, getQueryParameters));
    }

    private static TableReference tableReference(String baz) {
        return TableReference.create(Namespace.DEFAULT_NAMESPACE, baz);
    }

    private static GetQuerySpec getQuerySpec(
            String keyspace,
            TableReference tableReference,
            GetQueryParameters getQueryParameters) {
        return new GetQuerySpec(
                ImmutableCqlQueryContext.builder()
                        .keyspace(keyspace)
                        .tableReference(tableReference)
                        .build(),
                getQueryParameters);
    }
}
