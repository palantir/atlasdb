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

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Set;

public class AtlasConsoleServiceImpl implements AtlasConsoleService {

    private final AtlasDbService service;
    private final ObjectMapper mapper;

    public AtlasConsoleServiceImpl(AtlasDbService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    /*
     * Ideally, converting a query result to a *usable* form would be decoupled
     * from JSON serialization... but until then I guess we're passing JSON around.
     */

    @Override
    public String tables() throws IOException {
        Set<String> result = service.getAllTableNames();
        return toJson(result, Set.class);
    }

    @Override
    public String getMetadata(String table) throws IOException {
        TableMetadata result = service.getTableMetadata(table);
        return toJson(result, TableMetadata.class);
    }

    @Override
    public void truncate(String table) throws IOException {
        service.truncateTable(table);
    }

    @Override
    public String getRows(TransactionToken token, String data) throws IOException {
        TableRowSelection rows = fromJson(data, TableRowSelection.class);
        TableRowResult result = service.getRows(token, rows);
        return toJson(result, TableRowResult.class);
    }

    @Override
    public String getCells(TransactionToken token, String data) throws IOException {
        TableCell cells = fromJson(data, TableCell.class);
        TableCellVal result = service.getCells(token, cells);
        return toJson(result, TableCellVal.class);
    }

    @Override
    public String getRange(TransactionToken token, String data) throws IOException {
        TableRange range = fromJson(data, TableRange.class);
        RangeToken result = service.getRange(token, range);
        return toJson(result, RangeToken.class);
    }

    @Override
    public void put(TransactionToken token, String data) throws IOException {
        TableCellVal cells = fromJson(data, TableCellVal.class);
        service.put(token, cells);
    }

    @Override
    public void delete(TransactionToken token, String data) throws IOException {
        TableCell cells = fromJson(data, TableCell.class);
        service.delete(token, cells);
    }

    @Override
    public TransactionToken startTransaction() {
        return service.startTransaction();
    }

    @Override
    public void commit(TransactionToken token) {
        service.commit(token);
    }

    @Override
    public void abort(TransactionToken token) {
        service.abort(token);
    }

    private <T> String toJson(T value, Class<T> type) throws IOException {
        return mapper.writerWithType(type).writeValueAsString(value);
    }

    private <T> T fromJson(String data, Class<T> type) throws IOException {
        return mapper.getFactory().createParser(data).readValueAs(type);
    }


}
