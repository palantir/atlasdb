package com.palantir.atlasdb.performance.cli.backend;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

public abstract class PhysicalStore implements AutoCloseable {

    public static PhysicalStore create(PhysicalStoreType type) {
       switch (type) {
           case POSTGRES:
               return new PostgresStore();
           default:
               throw new EnumConstantNotPresentException(PhysicalStoreType.class, type.name());
       }
    }

    public abstract KeyValueService connect();

}
