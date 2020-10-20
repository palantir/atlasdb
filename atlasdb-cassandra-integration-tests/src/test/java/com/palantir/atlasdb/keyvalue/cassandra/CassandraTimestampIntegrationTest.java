/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.cassandra;

import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.containers.CassandraResource;
import com.palantir.flake.ShouldRetry;
import com.palantir.timestamp.MultipleRunningTimestampServiceError;
import com.palantir.timestamp.TimestampBoundStore;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

@ShouldRetry
public class CassandraTimestampIntegrationTest {
    @ClassRule
    public static final CassandraResource CASSANDRA = new CassandraResource();

    private CassandraKeyValueService kv = CASSANDRA.getDefaultKvs();

    @Before
    public void setUp() {
        kv.dropTable(AtlasDbConstants.TIMESTAMP_TABLE);
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
    public void resilientToMultipleStoreUpperLimitBeforeGet() {
        TimestampBoundStore ts = CassandraTimestampBoundStore.create(kv);
        long limit = ts.getUpperLimit();
        ts.storeUpperLimit(limit + 10);
        ts.storeUpperLimit(limit + 20);
        Assert.assertEquals(limit + 20, ts.getUpperLimit());
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
