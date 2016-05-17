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

/**
 * OracleShim allows Oracle dependent logic to be injected into the SQL
 * dependent classes that support both Legacy DB and AtlasDB's Dbkvs
 */

public interface OracleShim {
    public interface OracleBlob {
        void freeTemporary() throws SQLException;

        OutputStream setBinaryStream(int i) throws SQLException;

        Blob getBlob();
    }

    public interface OracleStructArray {
        Object toOracleArray(Connection c) throws SQLException;
    }

    OracleStructArray createOracleStructArray(String structType,
                                              String arrayType,
                                              List<Object[]> elements);

    OracleBlob createOracleBlob(Connection c) throws SQLException;
}