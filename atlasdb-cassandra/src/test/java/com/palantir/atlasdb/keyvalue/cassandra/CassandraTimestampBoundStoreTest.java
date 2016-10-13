/**
 * Copyright 2016 Palantir Technologies
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

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class CassandraTimestampBoundStoreTest {
    @Test
    public void testSetAndGet() {
        TimestampDao dao = new InMemoryTimestampDao();
        CassandraTimestampBoundStore store = new CassandraTimestampBoundStore(dao);

        store.storeUpperLimit(5);

        assertEquals(store.getUpperLimit(), 5);
    }

    @Test
    public void raceCondition() throws InterruptedException {
        TimestampDao dao = new InMemoryTimestampDao();
        CassandraTimestampBoundStore store = new CassandraTimestampBoundStore(dao);
        ExecutorService service = Executors.newFixedThreadPool(2);

        service.submit(store::getUpperLimit);
        service.submit(() -> store.storeUpperLimit(5));

        service.awaitTermination(5, TimeUnit.SECONDS);

        store.storeUpperLimit(5);
    }
}
