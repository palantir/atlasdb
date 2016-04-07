package com.palantir.nexus.db.sql;


import java.io.InputStream;

import org.joda.time.DateTime;

import com.palantir.exception.PalantirSqlException;

public interface AgnosticLightResultRow extends AgnosticResultRow {

    InputStream getBinaryInputStream(final String colname) throws PalantirSqlException;

    DateTime getDateTime(String colname) throws PalantirSqlException;

}
