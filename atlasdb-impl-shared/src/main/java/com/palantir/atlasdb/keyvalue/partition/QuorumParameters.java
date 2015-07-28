package com.palantir.atlasdb.keyvalue.partition;

import com.google.common.base.Preconditions;
import com.palantir.common.annotation.Immutable;

@Immutable public final class QuorumParameters {
    final int replicationFactor;
    final int readFactor;
    final int writeFactor;

    @Immutable public final static class QuorumRequestParameters {
        final int replicationFator;
        final int successFactor;

        public int getReplicationFator() {
            return replicationFator;
        }

        public int getSuccessFactor() {
            return successFactor;
        }

        public int getFailureFactor() {
            return replicationFator - successFactor;
        }

        private QuorumRequestParameters(int replicationFactor, int successFactor) {
            this.replicationFator = replicationFactor;
            this.successFactor = successFactor;
        }
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public int getReadFactor() {
        return readFactor;
    }

    public int getWriteFactor() {
        return writeFactor;
    }

    public QuorumRequestParameters getReadRequestParameters() {
        return new QuorumRequestParameters(replicationFactor, readFactor);
    }

    public QuorumRequestParameters getWriteRequestParameters() {
        return new QuorumRequestParameters(replicationFactor, writeFactor);
    }

    public QuorumRequestParameters getNoFailureRequestParameters() {
        return new QuorumRequestParameters(replicationFactor, replicationFactor);
    }

    public QuorumParameters(int replicationFactor, int readFactor, int writeFactor) {
        Preconditions.checkArgument(readFactor + writeFactor > replicationFactor);
        this.replicationFactor = replicationFactor;
        this.readFactor = readFactor;
        this.writeFactor = writeFactor;
    }
}
