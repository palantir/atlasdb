/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.keyvalue.TableMappingService;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.keyvalue.impl.KVTableMappingService;

public class TableMapperTest {
    private KeyValueService keyValueService;
    private TableMappingService tableMappingService;

    @Before
    public void setUpTableMap() throws Exception {
        keyValueService = new InMemoryKeyValueService(false);
        keyValueService.initializeFromFreshInstance();
        final AtomicLong counter = new AtomicLong();
        tableMappingService = KVTableMappingService.create(keyValueService, new Supplier<Long>() {
            @Override
            public Long get() {
                return counter.incrementAndGet();
            }
        });
    }

    @After
    public void teardown() {
        keyValueService.teardown();
    }

    @Test
    public void testAddIdempotent() throws Exception {
        TableReference table1 = TableReference.create(Namespace.create("test"), "newTable");
        tableMappingService.addTable(table1);
        assertEquals(table1,
                Iterables.getOnlyElement(tableMappingService.mapToFullTableNames(ImmutableSet.of(tableMappingService.getShortTableName(table1)))));
        tableMappingService.addTable(table1);
        assertEquals(table1,
                Iterables.getOnlyElement(tableMappingService.mapToFullTableNames(ImmutableSet.of(tableMappingService.getShortTableName(table1)))));

    }

    @Test
    public void testInvalidTableReference() throws Exception {
        try {
            tableMappingService.getShortTableName(TableReference.create(Namespace.create("invalid"), "invalidTable"));
            fail();
        } catch (IllegalArgumentException e) {
            // intended to fail on non-existent tables
        }
    }
}
