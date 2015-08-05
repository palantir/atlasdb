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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;

public class CassandraTimestampTest {
    private static final String TIMESTAMP_TABLE = "_timestamp";

    private CassandraKeyValueService kv;

    @Before
    public void setUp() {
        kv = CassandraKeyValueService.create(
                ImmutableSet.of("localhost"),
                9160,
                20,
                "atlasdb", false,
                1,
                10000,
                10000000,
                1000,
                true,
                false,
                null);
        kv.initializeFromFreshInstance();
        kv.dropTable(TIMESTAMP_TABLE);
    }

    public void tearDown() {
        kv.teardown();
    }

    @Test
    public void testBounds() {
        TimestampBoundStore ts = CassandraTimestampBoundStore.create(kv);
        long limit = ts.getUpperLimit();
        ts.storeUpperLimit(limit + 10);
        Assert.assertEquals(limit + 10, ts.getUpperLimit());
        ts.storeUpperLimit(limit + 20);
        Assert.assertEquals(limit + 20, ts.getUpperLimit());
        ts.storeUpperLimit(limit + 30);
        Assert.assertEquals(limit + 30, ts.getUpperLimit());
    }

    @Test
    public void testMultipleThrows() {
        TimestampBoundStore ts = CassandraTimestampBoundStore.create(kv);
        TimestampBoundStore ts2 = CassandraTimestampBoundStore.create(kv);
        long limit = ts.getUpperLimit();
        Assert.assertEquals(limit, ts2.getUpperLimit());
        ts.storeUpperLimit(limit + 10);
        Assert.assertEquals(limit + 10, ts.getUpperLimit());
        Assert.assertEquals(limit + 10, ts2.getUpperLimit());

        ts.storeUpperLimit(limit + 20);
        try {
            ts2.storeUpperLimit(limit + 20);
            Assert.fail();
        } catch (MultipleRunningTimestampServiceError e) {
            // expected
        }
        Assert.assertEquals(limit + 20, ts.getUpperLimit());
        Assert.assertEquals(limit + 20, ts2.getUpperLimit());

        ts.storeUpperLimit(limit + 30);
        Assert.assertEquals(limit + 30, ts.getUpperLimit());

        try {
            ts2.storeUpperLimit(limit + 40);
            Assert.fail();
        } catch (MultipleRunningTimestampServiceError e) {
            // expected
        }
    }
}
