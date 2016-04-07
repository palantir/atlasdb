package com.palantir.nexus.db.sql;

import com.palantir.exception.PalantirSqlException;

public interface PalantirSequenceEnabledSqlConnection extends PalantirSqlConnection {
    long selectID(String sequenceName) throws PalantirSqlException;
    long [] selectIDs(String sequenceName, int num) throws PalantirSqlException;
}
