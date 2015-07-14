/*
 * Copyright 2014 Palantir Technologies, Inc. All rights reserved.
 */

package com.palantir.atlasdb.rdbms.api.service;

public abstract class AtlasRdbmsConnectionCheckedCallable<V, K extends Exception> implements AtlasRdbmsConnectionCallable<V> {
    private final Class<K> throwableClass;

    protected AtlasRdbmsConnectionCheckedCallable(Class<K> throwableClass) {
        this.throwableClass = throwableClass;
    }

    public Class<K> getThrowableClass() {
        return throwableClass;
    }

    @Override
    public abstract V call(AtlasRdbmsConnection c) throws Exception, K;
}
