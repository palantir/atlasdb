package com.palantir.atlasdb.performance.backend;

import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.performance.BenchmarkParam;

@State(Scope.Benchmark)
public class KeyValueServiceConnector extends PhysicalStore {

    /**
     * Edit this instance variable name ("backend") with care -- it must match {@link BenchmarkParam.BACKEND}.getKey()
     */
    @Param
    private KeyValueServiceType backend;

    private PhysicalStore store;

    @Override
    public KeyValueService connect() {
        if (store != null) {
            throw new IllegalStateException("connect() has already been called");
        }
        store = PhysicalStore.create(backend);
        return store.connect();
    }

    @Override
    public void close() throws Exception {
        if (store != null) {
            store.close();
        }
    }
}
