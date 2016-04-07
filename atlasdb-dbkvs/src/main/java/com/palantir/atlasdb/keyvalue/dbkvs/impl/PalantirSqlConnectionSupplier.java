package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import java.io.Closeable;

import com.google.common.base.Supplier;
import com.palantir.exception.PalantirSqlException;
import com.palantir.nexus.db.sql.PalantirSqlConnection;

public interface PalantirSqlConnectionSupplier extends Supplier<PalantirSqlConnection>, Closeable {

    @Override
    public PalantirSqlConnection get();

    @Override
    public void close() throws PalantirSqlException;

}
