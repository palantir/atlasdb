package com.palantir.atlasdb.keyvalue.dbkvs.impl;

public enum TableSize {
    RAW(3), OVERFLOW(4);

    private final int id;

    private TableSize(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public static TableSize byId(int id) {
        switch (id) {
        case 3: return RAW;
        case 4: return OVERFLOW;
        }
        throw new IllegalArgumentException("Unknown table size: " + id);
    }
}
