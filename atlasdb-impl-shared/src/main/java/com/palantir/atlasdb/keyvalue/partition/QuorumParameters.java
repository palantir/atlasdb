package com.palantir.atlasdb.keyvalue.partition;

import com.google.common.base.Preconditions;
import com.palantir.common.annotation.Immutable;

@Immutable public final class QuorumParameters {
    final int replicationFactor;
    final int readFactor;
    final int writeFactor;

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public int getReadFactor() {
        return readFactor;
    }

    public int getWriteFactor() {
        return writeFactor;
    }

    public QuorumParameters(int replicationFactor, int readFactor, int writeFactor) {
        Preconditions.checkArgument(readFactor + writeFactor > replicationFactor);
        this.replicationFactor = replicationFactor;
        this.readFactor = readFactor;
        this.writeFactor = writeFactor;
    }
}
