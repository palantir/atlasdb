package com.palantir.atlas.api;

public class TransactionToken {
    private final long id;

    public TransactionToken(long id) {
        this.id = id;
    }

    public static TransactionToken autoCommit() {
        return new TransactionToken(-1);
    }

    public long getId() {
        return id;
    }

    public boolean shouldAutoCommit() {
        return id < 0;
    }

    @Override
    public String toString() {
        return "TransactionToken [id=" + id + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
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
        TransactionToken other = (TransactionToken) obj;
        if (id != other.id)
            return false;
        return true;
    }
}
