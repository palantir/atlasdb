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
package com.palantir.atlasdb.transaction.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class ConflictDetectionManagerTest {

    private static final TableReference TABLE_1 = TableReference.fromString("default.table1");
    private static final TableReference TABLE_2 = TableReference.fromString("default.table2");
    private static final TableReference TABLE_3 = TableReference.fromString("default.table3");
    private static final TableReference TABLE_4 = TableReference.fromString("default.table4");

    private Map<TableReference, ConflictHandler> map;
    private AtomicInteger callCount;
    private ConflictDetectionManager manager;

    @Before
    public void setUp() {
        map = new HashMap<>();
        callCount = new AtomicInteger(0);
        manager = new ConflictDetectionManager(() -> {
            callCount.incrementAndGet();
            return map;
        });
    }

    @Test
    public void testEmptyManager() {
        Assert.assertTrue(manager.isEmptyOrContainsTable(TableReference.fromString("default.table")));
        Assert.assertTrue(manager.get().isEmpty());
        manager.recompute();
        Assert.assertTrue(manager.get().isEmpty());
    }

    @Test
    public void testRecomputeNoOverrides() {
        map.put(TABLE_1, ConflictHandler.IGNORE_ALL);
        map.put(TABLE_3, ConflictHandler.IGNORE_ALL);

        // Initial get() loads supplier properly
        Assert.assertEquals(ImmutableMap.of(
                        TABLE_1, ConflictHandler.IGNORE_ALL,
                        TABLE_3, ConflictHandler.IGNORE_ALL),
                manager.get());
        Assert.assertEquals(1, callCount.get());

        map.clear();
        map.put(TABLE_1, ConflictHandler.RETRY_ON_VALUE_CHANGED);
        map.put(TABLE_2, ConflictHandler.IGNORE_ALL);

        // results are cached
        Assert.assertEquals(ImmutableMap.of(
                        TABLE_1, ConflictHandler.IGNORE_ALL,
                        TABLE_3, ConflictHandler.IGNORE_ALL),
                manager.get());
        Assert.assertEquals(1, callCount.get());

        manager.recompute();

        // recompute() recomputes the cached result
        Assert.assertEquals(ImmutableMap.of(
                        TABLE_1, ConflictHandler.RETRY_ON_VALUE_CHANGED,
                        TABLE_2, ConflictHandler.IGNORE_ALL),
                manager.get());
        Assert.assertEquals(2, callCount.get());
    }

    @Test
    public void testWithOverrides() {
        map.put(TABLE_1, ConflictHandler.IGNORE_ALL);
        map.put(TABLE_3, ConflictHandler.IGNORE_ALL);

        Assert.assertEquals(ImmutableMap.of(
                TABLE_1, ConflictHandler.IGNORE_ALL,
                TABLE_3, ConflictHandler.IGNORE_ALL),
                manager.get());

        manager.setConflictDetectionMode(TABLE_1, ConflictHandler.RETRY_ON_VALUE_CHANGED);
        manager.setConflictDetectionMode(TABLE_2, ConflictHandler.RETRY_ON_VALUE_CHANGED);

        Assert.assertEquals(ImmutableMap.of(
                TABLE_1, ConflictHandler.RETRY_ON_VALUE_CHANGED,
                TABLE_2, ConflictHandler.RETRY_ON_VALUE_CHANGED,
                TABLE_3, ConflictHandler.IGNORE_ALL),
                manager.get());

        manager.removeConflictDetectionMode(TABLE_1);

        Assert.assertEquals(ImmutableMap.of(
                TABLE_1, ConflictHandler.IGNORE_ALL,
                TABLE_2, ConflictHandler.RETRY_ON_VALUE_CHANGED,
                TABLE_3, ConflictHandler.IGNORE_ALL),
                manager.get());
    }

    @Test
    public void testIsEmptyOrContainsTable() {
        map.put(TABLE_1, ConflictHandler.IGNORE_ALL);
        map.put(TABLE_3, ConflictHandler.IGNORE_ALL);

        manager.setConflictDetectionMode(TABLE_1, ConflictHandler.RETRY_ON_VALUE_CHANGED);
        manager.setConflictDetectionMode(TABLE_2, ConflictHandler.RETRY_ON_VALUE_CHANGED);

        int startCount = callCount.get();
        Assert.assertTrue(manager.isEmptyOrContainsTable(TABLE_1));
        Assert.assertTrue(manager.isEmptyOrContainsTable(TABLE_2));
        Assert.assertTrue(manager.isEmptyOrContainsTable(TABLE_3));
        Assert.assertFalse(manager.isEmptyOrContainsTable(TABLE_4));
        Assert.assertEquals(startCount, callCount.get());
    }
}