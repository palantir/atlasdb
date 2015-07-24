package com.palantir.atlasdb.transaction.service;

public class TransactionLogEntry {
    private final long startTimestamp;
    private final long commitTimestamp;

    public TransactionLogEntry(long startTimestamp, long commitTimestamp) {
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (commitTimestamp ^ (commitTimestamp >>> 32));
        result = prime * result + (int) (startTimestamp ^ (startTimestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TransactionLogEntry other = (TransactionLogEntry) obj;
        if (commitTimestamp != other.commitTimestamp)
            return false;
        if (startTimestamp != other.startTimestamp)
            return false;
        return true;
    }
}
