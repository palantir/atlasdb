/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.db.oracle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.BaseEncoding;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import oracle.jdbc.OracleConnection;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.BLOB;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

/**
 *  Implements the Oracle dependent logic required by underlying SQL implementations.
 */
public class NativeOracleJdbcHandler implements JdbcHandler {

    @Override
    public ArrayHandler createStructArray(String structType, String arrayType, List<Object[]> elements) {
        return new ArrayHandler() {
            private ARRAY array = null;

            @Override
            public ARRAY toOracleArray(Connection connection) throws SQLException {
                OracleConnection oracleConnection = getOracleConnection(connection);
                StructDescriptor sd = StructDescriptor.createDescriptor(structType, oracleConnection);
                ArrayDescriptor ad = ArrayDescriptor.createDescriptor(arrayType, oracleConnection);

                STRUCT[] structs = new STRUCT[elements.size()];
                for (int i = 0; i < elements.size(); ++i) {
                    structs[i] = new STRUCT(sd, oracleConnection, elements.get(i));
                }
                array = new ARRAY(ad, oracleConnection, structs);
                return array;
            }

            @Override
            public String toString() {
                if (array != null && array.getBytes() != null) {
                    return BaseEncoding.base16().encode(array.getBytes());
                } else {
                    return "[]";
                }
            }
        };
    }

    @Override
    public BlobHandler createBlob(Connection connection) throws SQLException {
        OracleConnection oracleConnection = getOracleConnection(connection);
        BLOB blob = BLOB.createTemporary(oracleConnection, false, BLOB.DURATION_SESSION);

        return new BlobHandler() {
            @Override
            public void freeTemporary() throws SQLException {
                BLOB.freeTemporary(blob);
            }

            @Override
            public OutputStream setBinaryStream(int num) throws SQLException {
                return blob.setBinaryStream(num);
            }

            @Override
            public Blob getBlob() {
                return blob;
            }

            @Override
            public String toString() {
                if (blob != null && blob.getBytes() != null) {
                    return BaseEncoding.base16().encode(blob.getBytes());
                } else {
                    return "(empty blob)";
                }
            }
        };
    }

    @VisibleForTesting
    static OracleConnection getOracleConnection(Connection connection) throws SQLException {
        if (connection instanceof OracleConnection) {
            return (OracleConnection) connection;
        } else if (connection.isWrapperFor(OracleConnection.class)) {
            return connection.unwrap(OracleConnection.class);
        } else {
            throw new ClassCastException("Cannot get " + OracleConnection.class.getName() + " from " + connection);
        }
    }
}
