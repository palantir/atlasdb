package com.palantir.nexus.db.pool;

public interface ResourceType<R, E extends Exception> {
    void close(R resource) throws E;
    R closeWrapper(R delegate, ResourceOnClose<E> onClose);
    String name();
}
