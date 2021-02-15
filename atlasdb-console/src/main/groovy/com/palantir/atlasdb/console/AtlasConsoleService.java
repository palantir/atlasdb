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

public interface AtlasConsoleService {
    String tables() throws IOException;

    String getMetadata(String table) throws IOException;

    String getRows(TransactionToken token, String data) throws IOException;

    String getCells(TransactionToken token, String data) throws IOException;

    String getRange(TransactionToken token, String data) throws IOException;

    void put(TransactionToken token, String data) throws IOException;

    void delete(TransactionToken token, String data) throws IOException;

    void truncate(String table) throws IOException;

    TransactionToken startTransaction();

    void commit(TransactionToken token);

    void abort(TransactionToken token);
}
