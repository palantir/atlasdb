package com.palantir.atlasdb.keyvalue.rocksdb.impl;

import java.io.Closeable;
import java.util.List;

import org.rocksdb.RocksObject;

import com.google.common.collect.Lists;

public class Disposer implements Closeable {
    private final List<RocksObject> disposables = Lists.newArrayListWithCapacity(2);

    public <T extends RocksObject> T register(T disposable) {
        this.disposables.add(disposable);
        return disposable;
    }

    @Override
    public void close() {
        for (RocksObject disposable : disposables) {
            disposable.dispose();
        }
    }
}
