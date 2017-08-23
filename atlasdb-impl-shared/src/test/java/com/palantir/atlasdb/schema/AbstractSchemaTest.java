/*
 * Copyright 2017 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableSet;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.Namespace;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.GenericTestSchema;
import com.palantir.atlasdb.table.description.Schema;
import com.palantir.atlasdb.table.description.generated.GenericTestSchemaTableFactory;
import com.palantir.atlasdb.table.description.generated.GenericTestTable;
import com.palantir.atlasdb.transaction.impl.AbstractTransaction;


public abstract class AbstractSchemaTest {
    private static final Schema schema = GenericTestSchema.getSchema();
    private static final TableReference tableRef =
            TableReference.create(Namespace.DEFAULT_NAMESPACE, "GenericTest");
    private static final GenericTestSchemaTableFactory tableFactory = GenericTestSchemaTableFactory.of();

    private String testRowKey = "testRowKey";
    private String testFirstColShortName = "c";
    private String testSecondColShortName = "d";
    private long testValue = 2L;

    @Test
    public void testPutSingleRowFirstColumn() {
        AbstractTransaction t = mock(AbstractTransaction.class);
        GenericTestTable table = tableFactory.getGenericTestTable(t);
        putSingleRowFirstColumn(table, testRowKey, testValue);

        ArgumentCaptor<Map> argument = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(t, times(1)).useTable(tableRef, table);
        Mockito.verify(t, times(1)).put(eq(tableRef), argument.capture());

        Cell expectedCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(testFirstColShortName));
        assertEquals(argument.getValue().entrySet().size(), 1);
        assertEquals(argument.getValue().keySet().toArray()[0], expectedCell);
        assertTrue(Arrays.equals((byte[]) argument.getValue().values().toArray()[0],
                EncodingUtils.encodeUnsignedVarLong(testValue)));
    }

    @Test
    public void testGetSingleRowAllColumns() {
        AbstractTransaction t = mock(AbstractTransaction.class);
        GenericTestTable table = tableFactory.getGenericTestTable(t);
        getSingleRowAllColumns(table, testRowKey);

        ColumnSelection allColumns = ColumnSelection.create(
                Arrays.asList(PtBytes.toCachedBytes("c"), PtBytes.toCachedBytes("d")));
        ArgumentCaptor<ArrayList> argument = ArgumentCaptor.forClass(ArrayList.class);
        Mockito.verify(t, times(1)).getRows(eq(tableRef), argument.capture(), eq(allColumns));

        assertEquals(argument.getValue().size(), 1);
        assertTrue(Arrays.equals((byte[]) argument.getValue().toArray()[0], PtBytes.toBytes(testRowKey)));
    }

    @Test
    public void testGetSingleRowFirstColumn() {
        AbstractTransaction t = mock(AbstractTransaction.class);
        GenericTestTable table = tableFactory.getGenericTestTable(t);
        getSingleRowFirstColumn(table, testRowKey);

        ColumnSelection allColumns = ColumnSelection.create(Collections.singletonList(PtBytes.toCachedBytes("c")));
        ArgumentCaptor<ArrayList> argument = ArgumentCaptor.forClass(ArrayList.class);
        Mockito.verify(t, times(1)).getRows(eq(tableRef), argument.capture(), eq(allColumns));

        assertEquals(argument.getValue().size(), 1);
        assertTrue(Arrays.equals((byte[]) argument.getValue().toArray()[0], PtBytes.toBytes(testRowKey)));
    }

    @Test
    public void testDeleteWholeRow() {
        AbstractTransaction t = mock(AbstractTransaction.class);
        GenericTestTable table = tableFactory.getGenericTestTable(t);
        deleteWholeRow(table, testRowKey);

        ArgumentCaptor<Set> argument = ArgumentCaptor.forClass(Set.class);
        Mockito.verify(t, times(1)).delete(eq(tableRef), argument.capture());

        Cell deletedFirstCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(testFirstColShortName));
        Cell deletedSecondCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(testSecondColShortName));
        assertEquals(argument.getValue().size(), 2);
        assertEquals(argument.getValue(), ImmutableSet.of(deletedFirstCell, deletedSecondCell));
    }

    @Test
    public void testDeleteFirstColumn() {
        AbstractTransaction t = mock(AbstractTransaction.class);
        GenericTestTable table = tableFactory.getGenericTestTable(t);
        deleteFirstColumn(table, testRowKey);

        ArgumentCaptor<Set> argument = ArgumentCaptor.forClass(Set.class);
        Mockito.verify(t, times(1)).delete(eq(tableRef), argument.capture());

        Cell deletedCell = Cell.create(PtBytes.toBytes(testRowKey), PtBytes.toBytes(testFirstColShortName));
        assertEquals(argument.getValue().size(), 1);
        assertEquals(argument.getValue(), ImmutableSet.of(deletedCell));
    }

    protected abstract void putSingleRowFirstColumn(GenericTestTable table, String roWKey, long value);
    protected abstract void getSingleRowAllColumns(GenericTestTable table, String rowKey);
    protected abstract void getSingleRowFirstColumn(GenericTestTable table, String rowKey);
    protected abstract void deleteWholeRow(GenericTestTable table, String rowKey);
    protected abstract void deleteFirstColumn(GenericTestTable table, String rowKey);
}
