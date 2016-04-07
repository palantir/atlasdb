package com.palantir.atlasdb.keyvalue.dbkvs.impl;

public interface DbMetadataTable {
    boolean exists();
    byte[] getMetadata();
    void putMetadata(byte[] metadata);
}
