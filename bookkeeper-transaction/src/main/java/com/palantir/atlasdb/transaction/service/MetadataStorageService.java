package com.palantir.atlasdb.transaction.service;

public interface MetadataStorageService {
    void put(String name, byte[] data);

    byte[] get(String name);
}
