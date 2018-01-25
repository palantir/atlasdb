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

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestRule;

import com.palantir.atlasdb.containers.CassandraContainer;
import com.palantir.atlasdb.containers.Containers;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.transaction.impl.AbstractTransactionTest;
import com.palantir.flake.FlakeRetryingRule;
import com.palantir.flake.ShouldRetry;

@ShouldRetry // The first test can fail with a TException: No host tried was able to create the keyspace requested.
public class CassandraKeyValueServiceTransactionIntegrationTest extends AbstractTransactionTest {
    @ClassRule
    public static final Containers CONTAINERS = new Containers(CassandraKeyValueServiceTransactionIntegrationTest.class)
            .with(new CassandraContainer());

    @Rule
    public final TestRule flakeRetryingRule = new FlakeRetryingRule();

    @Override
    protected KeyValueService getKeyValueService() {
        return CassandraKeyValueServiceImpl.create(
                CassandraContainer.KVS_CONFIG,
                CassandraContainer.LEADER_CONFIG);
    }

    @Override
    protected boolean supportsReverse() {
        return false;
    }

}
