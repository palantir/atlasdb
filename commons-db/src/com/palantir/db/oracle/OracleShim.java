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