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
package com.palantir.atlasdb.cleaner;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.common.time.Clock;

@RunWith(Parameterized.class)
public class PuncherTest {
    private static final long GRANULARITY_MILLIS = 10;

    @Parameters
    public static Collection<Object[]> parameters() {
        InMemoryKeyValueService kvsPuncherStoreKvs = new InMemoryKeyValueService(false);
        InMemoryKeyValueService cachingKvsPuncherStoreKvs = new InMemoryKeyValueService(false);

        InMemoryPuncherStore inMemoryPuncherStore = InMemoryPuncherStore.create();
        PuncherStore puncherStore =
                KeyValueServicePuncherStore.create(kvsPuncherStoreKvs);
        CachingPuncherStore cachingInMemoryPuncherStore =
                CachingPuncherStore.create(InMemoryPuncherStore.create(), GRANULARITY_MILLIS);
        CachingPuncherStore cachingKeyValueServicePuncherStore = CachingPuncherStore.create(
                KeyValueServicePuncherStore.create(cachingKvsPuncherStoreKvs),
                GRANULARITY_MILLIS);
        Object[][] parameters = new Object[][] { { inMemoryPuncherStore, null },
                { puncherStore, kvsPuncherStoreKvs },
                { cachingInMemoryPuncherStore, null },
                { cachingKeyValueServicePuncherStore, cachingKvsPuncherStoreKvs } };
        return ImmutableList.copyOf(parameters);
    }

    private final PuncherStore puncherStore;
    private final KeyValueService kvs;

    private final Clock clock = new Clock() {
        @Override
        public long getTimeMillis() {
            return timeMillis;
        }
    };

    public PuncherTest(PuncherStore puncherStore, KeyValueService kvs) {
        this.puncherStore = puncherStore;
        this.kvs = kvs;
    }

    @After
    public void shutdownKvs() {
        if (kvs != null) {
            kvs.close();
        }
    }

    long timeMillis = 0;

    final long firstPunchTimestamp = 33L;
    final long secondPunchTimestamp = 35L;
    final long thirdPunchTimestamp = 37L;

    @Test
    public void test() {
        Puncher puncher = SimplePuncher.create(puncherStore, clock, Suppliers.ofInstance(10000L));
        Supplier<Long> timestampSupplier = puncher.getTimestampSupplier();

        timeMillis += 60000L;
        assertEquals(Long.MIN_VALUE, (long) timestampSupplier.get());
        timeMillis += 60000L;
        assertEquals(Long.MIN_VALUE, (long) timestampSupplier.get());
        timeMillis += 60000L;

        puncher.punch(firstPunchTimestamp);

        timeMillis += 60000L;
        assertEquals(firstPunchTimestamp, (long) timestampSupplier.get());

        puncher.punch(secondPunchTimestamp);

        assertEquals(firstPunchTimestamp, (long) timestampSupplier.get());
        timeMillis += 60000L;
        assertEquals(secondPunchTimestamp, (long) timestampSupplier.get());
        timeMillis += 10L;
        assertEquals(secondPunchTimestamp, (long) timestampSupplier.get());

        puncher.punch(thirdPunchTimestamp);

        assertEquals(secondPunchTimestamp, (long) timestampSupplier.get());
        timeMillis += 60000L;
        assertEquals(thirdPunchTimestamp, (long) timestampSupplier.get());
    }

    @Test
    public void testBigTimestamp() {
        Puncher puncher = SimplePuncher.create(puncherStore, clock, Suppliers.ofInstance(10000L));
        timeMillis = (1L << 60) - 3;
        puncher.punch(1L << 62);
    }
}
