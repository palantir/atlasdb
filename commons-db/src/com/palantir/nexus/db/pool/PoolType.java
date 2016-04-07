package com.palantir.nexus.db.pool;

import javax.annotation.Nullable;

/**
 * Extensibility point for adding differing connection pool libraries.
 *
 * @author dstipp
 */

public enum PoolType {
    HIKARICP;

    /**
     * Looks up a PoolType by name. Names are not case-sensitive.
     *
     * @param strName the name of the type to lookup, typically corresponds to the type name, e.g.
     *        PoolType.HIKARICP.toString();
     * @return the property PoolType, or null if none exists
     * @throws IllegalArgumentException if requesting invalid pool.
     */
    @Nullable
    public static PoolType getTypeByName(@Nullable String strName) throws IllegalArgumentException {
        if (strName == null) {
            return null;
        }
        return PoolType.valueOf(strName.toUpperCase());
    }

}