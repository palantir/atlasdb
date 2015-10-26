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
package com.palantir.atlasdb.keyvalue.remoting;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;
import com.palantir.atlasdb.keyvalue.impl.AbstractAtlasDbKeyValueServiceTest;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;

import io.dropwizard.testing.junit.DropwizardClientRule;

public class KeyValueServiceRemotingTest extends AbstractAtlasDbKeyValueServiceTest {

    final KeyValueService remoteKvs = RemotingKeyValueService.createServerSide(new InMemoryKeyValueService(
            false), Suppliers.ofInstance(-1L));

    @Rule
    public final DropwizardClientRule Rule = Utils.getRemoteKvsRule(remoteKvs);
    public static final ObjectMapper mapper = Utils.mapper;

    volatile KeyValueService localKvs;

    @Before
    public void setupHacks() throws Exception {
        Utils.setupRuleHacks(Rule);
    }

    @Test
    public void testSerialize() throws IOException {
        Cell cell = Cell.create(row0, column0);
        String serializedCell = mapper.writeValueAsString(cell);
        Cell cellDeserialized = mapper.readValue(serializedCell, Cell.class);
        assertEquals(cell, cellDeserialized);

        byte[] row = row0;
        String serializedRow = mapper.writeValueAsString(row);
        byte[] rowDeserialized = mapper.readValue(serializedRow, byte[].class);
        Assert.assertArrayEquals(row, rowDeserialized);

        Map<Cell, byte[]> cellMap = ImmutableMap.of(cell, value00);
        String serializedMap = mapper.writerFor(
                mapper.getTypeFactory().constructMapType(Map.class, Cell.class, byte[].class)).writeValueAsString(
                cellMap);
        Map<Cell, byte[]> cellMapDeserialized = mapper.readValue(
                serializedMap,
                mapper.getTypeFactory().constructMapType(Map.class, Cell.class, byte[].class));
        assertEquals(cellMap.size(), cellMapDeserialized.size());
        assertEquals(cellMap.keySet(), cellMapDeserialized.keySet());
        Assert.assertArrayEquals(value00, cellMapDeserialized.values().iterator().next());

        Value value = Value.create(value00, TEST_TIMESTAMP);
        String serializedValue = mapper.writeValueAsString(value);
        Value valueDeserialized = mapper.readValue(serializedValue, Value.class);
        assertEquals(valueDeserialized, value);

        SortedMap<byte[], Value> cells = ImmutableSortedMap.<byte[], Value> orderedBy(
                UnsignedBytes.lexicographicalComparator()).put(row0, value).build();
        RowResult<Value> rowResult = RowResult.create(row0, cells);
        String serializedRowResult = mapper.writeValueAsString(rowResult);
        JavaType rowResultType = mapper.getTypeFactory().constructParametrizedType(RowResult.class, RowResult.class, Value.class);
        RowResult<Value> rowResultDeserialized = mapper.readValue(serializedRowResult, rowResultType);
        assertEquals(rowResult, rowResultDeserialized);
    }

    @Test
    public void testBytes() throws IOException {
        String serializedEmptyByteArray = mapper.writeValueAsString(new byte[] {});
        byte[] readValue = mapper.readValue(serializedEmptyByteArray, byte[].class);
        Assert.assertArrayEquals(ArrayUtils.EMPTY_BYTE_ARRAY, readValue);
    }

    @Test
    public void testMeta() {
        byte[] someWeiredMeta = new byte[256];
        for (int i=0; i<256; ++i) {
            someWeiredMeta[i] = (byte) i;
        }
        getKeyValueService().putMetadataForTable(TEST_TABLE, someWeiredMeta);
        byte[] result = getKeyValueService().getMetadataForTable(TEST_TABLE);
        Assert.assertArrayEquals(someWeiredMeta, result);
        byte[] remoteResult = remoteKvs.getMetadataForTable(TEST_TABLE);
        Assert.assertArrayEquals(someWeiredMeta, remoteResult);
    }

    @Override
    protected KeyValueService getKeyValueService() {
        if (localKvs == null) {
            String uri = Rule.baseUri().toString();
            localKvs = RemotingKeyValueService.createClientSide(uri, Suppliers.ofInstance(-1L));
        }
        return Preconditions.checkNotNull(localKvs);
    }

}
