package com.palantir.atlasdb.rdbms.api.service;

/**
 * Represents the version of the schema of an RDBMS.
 * The version supports both a mainline major version
 * number and a hotfix version number.
 * @author mharris
 *
 */
public class AtlasRdbmsSchemaVersion implements Comparable<AtlasRdbmsSchemaVersion> {

    private final long majorVersion;
    private final long hotfixVersion;

    public AtlasRdbmsSchemaVersion(long majorVersion, long hotfixVersion) {
        this.majorVersion = majorVersion;
        this.hotfixVersion = hotfixVersion;
    }

    public long getMajorVersion() {
        return majorVersion;
    }

    public long getHotfixVersion() {
        return hotfixVersion;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (hotfixVersion ^ (hotfixVersion >>> 32));
        result = prime * result + (int) (majorVersion ^ (majorVersion >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AtlasRdbmsSchemaVersion other = (AtlasRdbmsSchemaVersion) obj;
        if (hotfixVersion != other.hotfixVersion) {
            return false;
        }
        if (majorVersion != other.majorVersion) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return majorVersion + "." + hotfixVersion;
    }

    @Override
    public int compareTo(AtlasRdbmsSchemaVersion o) {
        if (majorVersion < o.majorVersion) {
            return -1;
        } else if (majorVersion > o.majorVersion) {
            return -1;
        } else if (hotfixVersion < o.hotfixVersion) {
            return -1;
        } else if (hotfixVersion > o.hotfixVersion) {
            return 1;
        } else {
            return 0;
        }
    }
}
