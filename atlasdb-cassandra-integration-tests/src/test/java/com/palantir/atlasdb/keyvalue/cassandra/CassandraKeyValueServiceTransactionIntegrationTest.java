/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestRule;

import com.palantir.atlasdb.cassandra.CassandraMutationTimestampProviders;
import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
import com.palantir.exception.NotInitializedException;
import com.palantir.flake.FlakeRetryingRule;
import com.palantir.flake.ShouldRetry;
import com.palantir.timestamp.TimestampManagementService;

@ShouldRetry // The first test can fail with a TException: No host tried was able to create the keyspace requested.
public class CassandraKeyValueServiceTransactionIntegrationTest extends AbstractTransactionTest {
    private static final CassandraContainer container =
            new CassandraContainer(CassandraKeyValueServiceSweepTaskRunnerIntegrationTest.class);
    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraKeyValueServiceTransactionIntegrationTest.class)
            .with(container);

    // This constant exists so that fresh timestamps are always greater than the write timestamps of values used in the
    // test.
    private static final long ONE_BILLION = 1_000_000_000;

    @Rule
    public final TestRule flakeRetryingRule = new FlakeRetryingRule();

    @Before
    public void advanceTimestamp() {
        ((TimestampManagementService) timestampService).fastForwardTimestamp(ONE_BILLION);
    }

    @Override
    protected KeyValueService getKeyValueService() {
        return CassandraKeyValueServiceImpl.create(
                metricsManager,
                container.getConfig(),
                CassandraContainer.LEADER_CONFIG,
                CassandraMutationTimestampProviders.singleLongSupplierBacked(
                        () -> {
                            if (timestampService == null) {
                                throw new NotInitializedException("timestamp service");
                            }
                            return timestampService.getFreshTimestamp();
                        }));
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

}
