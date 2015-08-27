package com.palantir.atlasdb.keyvalue.partition.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class VersionedObject<T> {
    final long version;
    final T object;

    public long getVersion() {
        return version;
    }

    public T getObject() {
        return object;
    }

    private VersionedObject(T object, long version) {
        this.object = object;
        this.version = version;
    }

    @JsonCreator
    public static <T> VersionedObject<T> of(@JsonProperty("object") T object,
                                            @JsonProperty("version") long version) {
        return new VersionedObject<T>(object, version);
    }
}
