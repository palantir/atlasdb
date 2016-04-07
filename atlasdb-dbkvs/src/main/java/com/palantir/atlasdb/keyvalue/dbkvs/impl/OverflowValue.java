package com.palantir.atlasdb.keyvalue.dbkvs.impl;

public class OverflowValue {
    public final long ts;
    public final long id;
    public OverflowValue(long ts, long id) {
        this.ts = ts;
        this.id = id;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
        result = prime * result + (int) (ts ^ (ts >>> 32));
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
        OverflowValue other = (OverflowValue) obj;
        if (id != other.id)
            return false;
        if (ts != other.ts)
            return false;
        return true;
    }
}