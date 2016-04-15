package com.palantir.nexus.db.sql;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

/**
 * Encapsulates an Oracle ARRAY type containing STRUCT elements.
 */
public class OracleStructArray {
    private final String structType;
    private final String arrayType;
    private final List<Object[]> elements;

    /**
     * Creates an array.
     *
     * @param structType the name of the STRUCT type in Oracle.
     * @param arrayType the name of the ARRAY type in Oracle.
     * @param elements the elements of the array.
     */
    public OracleStructArray(String structType, String arrayType, List<Object[]> elements) {
        this.structType = structType;
        this.arrayType = arrayType;
        this.elements = elements;
    }

    /**
     * Constructs a ARRAY object suitable for passing to a statement/query.
     */
    public ARRAY toOracleArray(Connection c) throws SQLException {
        StructDescriptor sd = StructDescriptor.createDescriptor(structType, c);
        ArrayDescriptor ad = ArrayDescriptor.createDescriptor(arrayType, c);

        STRUCT[] structs = new STRUCT[elements.size()];
        for (int i = 0; i < elements.size(); ++i) {
            structs[i] = new STRUCT(sd, c, elements.get(i));
        }

        return new ARRAY(ad, c, structs);
    }
}
