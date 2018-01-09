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

import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.palantir.flake.FlakeRetryingRule;

/**
 * This test rule cleans up the schema mutation lock tables in the event of a test failure.
 * Note that this cleanup is done regardless of the nature of the failure.
 */
public class SchemaMutationLockReleasingRule implements TestRule {
    private final CassandraKeyValueService kvs;

    public SchemaMutationLockReleasingRule(CassandraKeyValueService kvs) {
        this.kvs = kvs;
    }

    public static RuleChain createChainedReleaseAndRetry(CassandraKeyValueService kvs) {
        // The ordering is important. If an attempt fails, we want to release the schema mutation lock BEFORE retrying.
        return RuleChain.outerRule(new FlakeRetryingRule())
                .around(new SchemaMutationLockReleasingRule(kvs));
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } catch (Throwable t) {
                    kvs.cleanUpSchemaMutationLockTablesState();
                    throw t;
                }
            }
        };
    }
}
