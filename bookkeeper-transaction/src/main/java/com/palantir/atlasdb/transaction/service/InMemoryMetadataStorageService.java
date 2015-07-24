package com.palantir.atlasdb.transaction.service;

import java.util.Map;

import com.google.common.collect.Maps;

public class InMemoryMetadataStorageService implements MetadataStorageService {
    private final Map<String, byte[]> metadataMap = Maps.newHashMap();

    @Override
    public synchronized void put(String name, byte[] data) {
        metadataMap.put(name, data);
    }

    @Override
    public synchronized byte[] get(String name) {
        return metadataMap.get(name);
    }
}
