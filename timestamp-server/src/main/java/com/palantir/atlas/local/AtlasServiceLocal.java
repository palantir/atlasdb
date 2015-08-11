package com.palantir.atlas.local;

import java.io.IOException;
import java.util.Collection;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.palantir.atlas.api.AtlasService;
import com.palantir.atlas.api.RangeToken;
import com.palantir.atlas.api.TableCell;
import com.palantir.atlas.api.TableCellVal;
import com.palantir.atlas.api.TableRange;
import com.palantir.atlas.api.TableRowResult;
import com.palantir.atlas.api.TableRowSelection;
import com.palantir.atlas.api.TransactionToken;
import com.palantir.atlas.impl.AtlasServerModule;
import com.palantir.atlasdb.table.description.TableMetadata;

public class AtlasServiceLocal {

    private final AtlasService service;
    private final ObjectMapper mapper;
    private static PalantirInjector injector;

    public AtlasServiceLocal(AtlasService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper = mapper;
    }

    public static AtlasServiceLocal init() {
        if(injector == null) {
            BindingSet bindings = PalantirModuleCompiler.compileModuleClass(AtlasServerModule.class);
            injector = PalantirInjectorImpl.create(bindings);
        }
        return new AtlasServiceLocal(injector.getInstance(AtlasService.class), injector.getInstance(ObjectMapper.class));
    }

    /*
     * Ideally, converting a query result to a *usable* form would be decoupled
     * from JSON serialization... but until then I guess we're passing JSON around.
     */

    public String tables() throws IOException {
        Collection<String> result = service.getAllTableNames();
        return toJson(result, new TypeReference<Collection<String>>() {});
    }

    public String getMetadata(String table) throws IOException {
        TableMetadata result = service.getTableMetadata(table);
        return toJson(result, TableMetadata.class);
    }

    public String getRows(TransactionToken token, String data) throws IOException {
        TableRowSelection rows = fromJson(data, TableRowSelection.class);
        TableRowResult result = service.getRows(token, rows);
        return toJson(result, TableRowResult.class);
    }

    public String getCells(TransactionToken token, String data) throws IOException {
        TableCell cells = fromJson(data, TableCell.class);
        TableCellVal result = service.getCells(token, cells);
        return toJson(result, TableCellVal.class);
    }

    public String getRange(TransactionToken token, String data) throws IOException {
        TableRange range = fromJson(data, TableRange.class);
        RangeToken result = service.getRange(token, range);
        return toJson(result, RangeToken.class);
    }

    public void put(TransactionToken token, String data) throws IOException {
        TableCellVal cells = fromJson(data, TableCellVal.class);
        service.put(token, cells);
    }

    public void delete(TransactionToken token, String data) throws IOException {
        TableCell cells = fromJson(data, TableCell.class);
        service.delete(token, cells);
    }

    public TransactionToken startTransaction() {
        return service.startTransaction();
    }

    public void commit(TransactionToken token) {
        service.commit(token);
    }

    public void abort(TransactionToken token) {
        service.abort(token);
    }

    private <T> String toJson(T value, Class<T> type) throws IOException {
        return mapper.writerWithType(type).writeValueAsString(value);
    }

    private <T> String toJson(T value, TypeReference<T> type) throws IOException {
        return mapper.writerWithType(type).writeValueAsString(value);
    }

    private <T> T fromJson(String data, Class<T> type) throws IOException {
        return mapper.getFactory().createParser(data).readValueAs(type);
    }
}
