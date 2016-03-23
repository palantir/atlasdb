package com.palantir.atlasdb.console;

import java.io.IOException;

import com.palantir.atlasdb.api.TransactionToken;

public class DisconnectedAtlasConsoleService implements AtlasConsoleService {

    public <T> T fail() {
        throw new RuntimeException("You are not connected to an atlas server. Please connect and retry.");
    }

    @Override
    public String tables() throws IOException {
        return fail();
    }

    @Override
    public String getMetadata(String table) throws IOException {
        return fail();
    }

    @Override
    public String getRows(TransactionToken token, String data) throws IOException {
        return fail();
    }

    @Override
    public String getCells(TransactionToken token, String data) throws IOException {
        return fail();
    }

    @Override
    public String getRange(TransactionToken token, String data) throws IOException {
        return fail();
    }

    @Override
    public void put(TransactionToken token, String data) throws IOException {
        fail();
    }

    @Override
    public void delete(TransactionToken token, String data) throws IOException {
        fail();
    }

    @Override
    public TransactionToken startTransaction() {
        return fail();
    }

    @Override
    public void commit(TransactionToken token) {
        fail();
    }

    @Override
    public void abort(TransactionToken token) {
        fail();
    }
}
