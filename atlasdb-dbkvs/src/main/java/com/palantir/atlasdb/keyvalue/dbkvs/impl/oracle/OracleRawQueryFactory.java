package com.palantir.atlasdb.keyvalue.dbkvs.impl.oracle;

import java.util.Collection;

import com.palantir.atlasdb.keyvalue.dbkvs.impl.FullQuery;
import com.palantir.atlasdb.keyvalue.dbkvs.impl.OverflowValue;
import com.palantir.db.oracle.OracleShim;

public class OracleRawQueryFactory extends OracleQueryFactory {

    public OracleRawQueryFactory(String tableName, OracleShim oracleShim) {
        super(tableName, oracleShim);
    }

    @Override
    String getValueSubselect(String tableAlias, boolean includeValue) {
        return includeValue ? ", " + tableAlias + ".val " : " ";
    }

    @Override
    public boolean hasOverflowValues() {
        return false;
    }

    @Override
    public Collection<FullQuery> getOverflowQueries(Collection<OverflowValue> overflowIds) {
        throw new IllegalStateException("raw tables don't have overflow fields");
    }
}
