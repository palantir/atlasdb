package com.palantir.atlas.impl;

import java.io.IOException;
import java.util.Collection;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.palantir.atlas.api.AtlasService;
import com.palantir.atlas.api.ExecutionResult;
import com.palantir.atlas.api.RangeToken;
import com.palantir.atlas.api.TableCell;
import com.palantir.atlas.api.TableCellVal;
import com.palantir.atlas.api.TableRange;
import com.palantir.atlas.api.TableRowResult;
import com.palantir.atlas.api.TableRowSelection;
import com.palantir.atlas.api.TransactionToken;
import com.palantir.atlasdb.table.description.TableMetadata;

public class AtlasServlet extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(AtlasServlet.class);
    private AtlasService atlasService;
    private ObjectMapper objectMapper;

    @Override
    public void init() throws ServletException {
        try {
            BindingSet bindings = PalantirModuleCompiler.compileModuleClass(AtlasServerModule.class);
            PalantirInjector injector = PalantirInjectorImpl.create(bindings);
            atlasService = injector.getInstance(AtlasService.class);
            objectMapper = injector.getInstance(ObjectMapper.class);
        } catch (Throwable t) {
            log.error("Failed to initialize atlas servlet", t);
            WrapperHelper.exit(1);
        }
    }

    @Override
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json; charset=UTF-8");
        String path = request.getPathInfo();
        if (path.startsWith("/tables")) {
            Collection<String> tableNames = atlasService.getAllTableNames();
            writeResponse(response, tableNames, new TypeReference<Collection<String>>() {});
        } else if (path.startsWith("/metadata")) {
            String tableName = path.substring("/metadata/".length());
            TableMetadata metadata = atlasService.getTableMetadata(tableName);
            writeResponse(response, metadata, TableMetadata.class);
        } else if (path.startsWith("/transaction")) {
            TransactionToken token = atlasService.startTransaction();
            writeResponse(response, token.getId(), Long.class);
        } else {
            throw new ServletException("Invalid URI: " + request.getRequestURI());
        }
    }

    @Override
    public void doPost(HttpServletRequest request,
                       final HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("application/json; charset=UTF-8");
        String path = request.getPathInfo();
        if (path.startsWith("/exec")) {
            String code = IOUtils.toString(request.getInputStream());
            ExecutionResult result = atlasService.exec(code);
            objectMapper.writer().writeValue(response.getWriter(), result);
            return;
        }
        TransactionToken token = getToken(path);
        JsonParser parser = objectMapper.getFactory().createParser(request.getInputStream());
        if (path.startsWith("/rows")) {
            TableRowSelection rows = parser.readValueAs(TableRowSelection.class);
            TableRowResult results = atlasService.getRows(token, rows);
            writeResponse(response, results, TableRowResult.class);
        } else if (path.startsWith("/cells")) {
            TableCell cells = parser.readValueAs(TableCell.class);
            TableCellVal results = atlasService.getCells(token, cells);
            writeResponse(response, results, TableCellVal.class);
        } else if (path.startsWith("/range")) {
            TableRange range = parser.readValueAs(TableRange.class);
            RangeToken result = atlasService.getRange(token, range);
            writeResponse(response, result, RangeToken.class);
        } else if (path.startsWith("/put")) {
            TableCellVal puts = parser.readValueAs(TableCellVal.class);
            atlasService.put(token, puts);
        } else if (path.startsWith("/delete")) {
            TableCell deletes = parser.readValueAs(TableCell.class);
            atlasService.delete(token, deletes);
        } else if (path.startsWith("/commit")) {
            atlasService.commit(token);
        } else if (path.startsWith("/abort")) {
            atlasService.abort(token);
        } else {
            throw new ServletException("Invalid URI: " + request.getRequestURI());
        }
    }

    private TransactionToken getToken(String path) {
        int index = path.indexOf('/', 1);
        if (index < 0 || index == path.length() - 1) {
            return TransactionToken.autoCommit();
        } else {
            return new TransactionToken(Long.parseLong(path.substring(index + 1)));
        }
    }

    private <T> void writeResponse(HttpServletResponse response, T value, Class<T> type) throws IOException {
        ObjectWriter writer = objectMapper.writerWithType(type);
        writer.writeValue(response.getWriter(), value);
    }

    private <T> void writeResponse(HttpServletResponse response, T value, TypeReference<T> type) throws IOException {
        ObjectWriter writer = objectMapper.writerWithType(type);
        writer.writeValue(response.getWriter(), value);
    }
}
