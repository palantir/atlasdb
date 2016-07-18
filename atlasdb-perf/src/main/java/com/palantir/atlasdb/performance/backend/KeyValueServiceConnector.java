package com.palantir.atlasdb.performance.backend;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;

@State(Scope.Benchmark)
public class KeyValueServiceConnector extends PhysicalStore {

    @Param
    private KeyValueServiceType type;

    private PhysicalStore store;

    @Override
    public KeyValueService connect() {
        if (store != null) {
            throw new IllegalStateException("connect() has already been called");
        }
        store = PhysicalStore.create(type);
        return store.connect();
    }

    @Override
    public void close() throws Exception {
        if (store != null) {
            store.close();
        }
    }
}
