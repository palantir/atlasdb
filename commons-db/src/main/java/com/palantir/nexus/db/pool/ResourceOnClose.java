package com.palantir.nexus.db.pool;

public interface ResourceOnClose<E extends Exception> {
    void close() throws E;
}
