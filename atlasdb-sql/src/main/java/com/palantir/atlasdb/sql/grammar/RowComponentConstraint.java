package com.palantir.atlasdb.sql.grammar;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.primitives.UnsignedBytes;

public class RowComponentConstraint {

    private final byte[] low; // inclusive
    private final byte[] high; // exclusive

    public RowComponentConstraint(@Nullable byte[] low, @Nullable byte[] high) {
        this.low = low != null ? low.clone() : null;
        this.high = high != null ? high.clone() : null;
    }

    public byte[] getLowerBound() {
        return low;
    }

    public byte[] getUpperBound() {
        return high;
    }

    public boolean isBoundedBelow() {
        return low != null;
    }

    public boolean isBoundedAbove() {
        return high != null;
    }

    public boolean isUnbounded() {
        return !isBoundedAbove() && !isBoundedBelow();
    }

    public RowComponentConstraint intersectWith(RowComponentConstraint bounds) {
        byte[] l = MoreObjects.firstNonNull(low, bounds.low);
        byte[] h = MoreObjects.firstNonNull(high, bounds.high);
        if (low != null && bounds.low != null) {
            l = UnsignedBytes.lexicographicalComparator().compare(low, bounds.low) < 0 ? low : bounds.low;
        }
        if (high != null && bounds.high != null) {
            h = UnsignedBytes.lexicographicalComparator().compare(high, bounds.high) < 0 ? high : bounds.high;
        }
        return new RowComponentConstraint(l, h);
    }
}
