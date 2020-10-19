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

import com.palantir.atlasdb.api.TransactionToken;
import java.io.IOException;

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
    public void truncate(String tableName) throws IOException {
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
