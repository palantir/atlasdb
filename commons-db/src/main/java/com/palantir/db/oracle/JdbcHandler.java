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
package com.palantir.db.oracle;

import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.auto.service.AutoService;

import io.dropwizard.jackson.Discoverable;

/**
 * JdbcHandler allows Oracle dependent logic to be injected into the SQL
 * dependent classes that support both Legacy DB and AtlasDB's Dbkvs
 */

@AutoService(Discoverable.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", visible = false)
public interface JdbcHandler {
    interface BlobHandler {
        void freeTemporary() throws SQLException;

        OutputStream setBinaryStream(int i) throws SQLException;

        Blob getBlob();
    }

    interface ArrayHandler {
        Object toOracleArray(Connection c) throws SQLException;
    }

    ArrayHandler createStructArray(String structType,
            String arrayType,
            List<Object[]> elements);

    BlobHandler createBlob(Connection c) throws SQLException;
}