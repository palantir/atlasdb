package com.palantir.atlasdb.rdbms.impl.util;

public interface IdCache {

    Iterable<Long> getIds(String sequenceName, int size);

    void invalidate();
}
