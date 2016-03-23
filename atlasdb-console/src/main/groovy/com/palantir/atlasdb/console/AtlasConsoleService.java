package com.palantir.atlasdb.console;

import java.io.IOException;

import com.palantir.atlasdb.api.TransactionToken;

public interface AtlasConsoleService {
    String tables() throws IOException;

    String getMetadata(String table) throws IOException;

    String getRows(TransactionToken token, String data) throws IOException;

    String getCells(TransactionToken token, String data) throws IOException;

    String getRange(TransactionToken token, String data) throws IOException;

    void put(TransactionToken token, String data) throws IOException;

    void delete(TransactionToken token, String data) throws IOException;

    TransactionToken startTransaction();

    void commit(TransactionToken token);

    void abort(TransactionToken token);
}
