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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.Before;
import org.junit.Test;

public class CassandraLegacyLockTest extends AbstractCassandraLockTest {
    @Override
    @Before
    public void setUp() {
        super.setUp();

        kvs.supportsCAS = false;
        slowTimeoutKvs.supportsCAS = false;
    }

    // it has a different timeout message
    @Test
    public void testLocksTimeout() throws InterruptedException, ExecutionException, TimeoutException {
        long id = kvs.waitForSchemaMutationLock();
        try {
            Future future = async(() -> {
                kvs.schemaMutationUnlock(kvs.waitForSchemaMutationLock());
            });
            exception.expect(ExecutionException.class);
            exception.expectMessage("unable to get a lock on Cassandra system schema mutations");
            future.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw e;
        } finally {
            kvs.schemaMutationUnlock(id);
        }
    }

}
