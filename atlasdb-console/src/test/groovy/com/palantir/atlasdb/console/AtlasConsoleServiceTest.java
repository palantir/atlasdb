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
package com.palantir.atlasdb.console;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.palantir.atlasdb.api.AtlasDbService;
import com.palantir.atlasdb.api.RangeToken;
import com.palantir.atlasdb.api.TableCell;
import com.palantir.atlasdb.api.TableCellVal;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.api.TableRowResult;
import com.palantir.atlasdb.api.TableRowSelection;
import com.palantir.atlasdb.api.TransactionToken;
import com.palantir.atlasdb.table.description.TableMetadata;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.Before;
import org.junit.Test;

public class AtlasConsoleServiceTest {
    private final Mockery context = new Mockery();
    private AtlasDbService delegate;
    private ObjectMapper mapper;
    private AtlasConsoleService service;
    private TransactionToken token;

    final String QUERY = "{'a': 'b'}";
    final String RESULT = "{'c': 'd'}";

    @Before
    public void setup() {
        context.setImposteriser(ClassImposteriser.INSTANCE);
        delegate = context.mock(AtlasDbService.class);
        mapper = context.mock(ObjectMapper.class);
        service = new AtlasConsoleServiceImpl(delegate, mapper);
        token = context.mock(TransactionToken.class);
    }

    private <T> Expectations toJson(final T output, final Class<T> clazz) throws JsonProcessingException {
        final ObjectWriter writer = context.mock(ObjectWriter.class);
        return new Expectations() {{
            oneOf(mapper).writerWithType(clazz); will(returnValue(writer));
            oneOf(writer).writeValueAsString(output); will(returnValue(RESULT));
        }};
    }

    private <T> Expectations fromJson(final T input, final Class<T> clazz) throws IOException {
        final JsonFactory factory = context.mock(JsonFactory.class);
        final JsonParser parser = context.mock(JsonParser.class);
        return new Expectations() {{
            oneOf(mapper).getFactory(); will(returnValue(factory));
            oneOf(factory).createParser(QUERY); will(returnValue(parser));
            oneOf(parser).readValueAs(clazz); will(returnValue(input));
        }};
    }

    @Test
    public void testTables() throws IOException {
        final Set<String> output = new HashSet<String>();
        context.checking(new Expectations() {{
            oneOf(delegate).getAllTableNames(); will(returnValue(output));
        }});
        context.checking(toJson(output, Set.class));
        assertEquals(service.tables(), RESULT);
        context.assertIsSatisfied();
    }

    @Test
    public void testGetMetadata() throws IOException {
        final TableMetadata output = context.mock(TableMetadata.class);
        final String table = "t";
        context.checking(new Expectations() {{
            oneOf(delegate).getTableMetadata(table); will(returnValue(output));
        }});
        context.checking(toJson(output, TableMetadata.class));
        assertEquals(service.getMetadata(table), RESULT);
        context.assertIsSatisfied();
    }

    @Test
    public void testGetRows() throws IOException {
        final TableRowSelection input = context.mock(TableRowSelection.class);
        final TableRowResult output = context.mock(TableRowResult.class);
        context.checking(fromJson(input, TableRowSelection.class));
        context.checking(new Expectations() {{
            oneOf(delegate).getRows(token, input); will(returnValue(output));
        }});
        context.checking(toJson(output, TableRowResult.class));
        assertEquals(service.getRows(token, QUERY), RESULT);
        context.assertIsSatisfied();
    }

    @Test
    public void testGetCells() throws IOException {
        final TableCell input = context.mock(TableCell.class);
        final TableCellVal output = context.mock(TableCellVal.class);
        context.checking(fromJson(input, TableCell.class));
        context.checking(new Expectations() {{
            oneOf(delegate).getCells(token, input); will(returnValue(output));

        }});
        context.checking(toJson(output, TableCellVal.class));
        assertEquals(service.getCells(token, QUERY), RESULT);
        context.assertIsSatisfied();
    }

    @Test
    public void testGetRange() throws IOException {
        final TableRange input = context.mock(TableRange.class);
        final RangeToken output = context.mock(RangeToken.class);
        context.checking(fromJson(input, TableRange.class));
        context.checking(new Expectations() {{
            oneOf(delegate).getRange(token, input); will(returnValue(output));
        }});
        context.checking(toJson(output, RangeToken.class));
        assertEquals(service.getRange(token, QUERY), RESULT);
        context.assertIsSatisfied();
    }

    @Test
    public void testPut() throws IOException {
        final TableCellVal input = context.mock(TableCellVal.class);
        context.checking(fromJson(input, TableCellVal.class));
        context.checking(new Expectations() {{
            oneOf(delegate).put(token, input);
        }});
        service.put(token, QUERY);
        context.assertIsSatisfied();
    }

    @Test
    public void testDelete() throws IOException {
        final TableCell input = context.mock(TableCell.class);
        context.checking(fromJson(input, TableCell.class));
        context.checking(new Expectations() {{
            oneOf(delegate).delete(token, input);
        }});
        service.delete(token, QUERY);
        context.assertIsSatisfied();
    }

    @Test
    public void testCommit() throws IOException {
        context.checking(new Expectations() {{
            oneOf(delegate).commit(token);
        }});
        service.commit(token);
        context.assertIsSatisfied();
    }

    @Test
    public void testAbort() throws IOException {
        context.checking(new Expectations() {{
            oneOf(delegate).abort(token);
        }});
        service.abort(token);
        context.assertIsSatisfied();
    }
}
