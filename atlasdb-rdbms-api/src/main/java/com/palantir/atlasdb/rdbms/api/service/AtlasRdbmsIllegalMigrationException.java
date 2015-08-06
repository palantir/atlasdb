package com.palantir.atlasdb.rdbms.api.service;

/**
 * An exception thrown when a client attempts to migrate
 * an AtlasRdbms schema from a later version to an earlier
 * version.
 * @author mharris
 *
 */
public class AtlasRdbmsIllegalMigrationException extends Exception {
    private static final long serialVersionUID = 1L;

    private final AtlasRdbmsSchemaVersion fromVersion;
    private final AtlasRdbmsSchemaVersion toVersion;

    public AtlasRdbmsIllegalMigrationException(AtlasRdbmsSchemaVersion fromVersion,
                                               AtlasRdbmsSchemaVersion toVersion) {
        super("Can't migrate from version: " + fromVersion + " to: " + toVersion + ".  Can only migrate forward in version");
        this.fromVersion = fromVersion;
        this.toVersion = toVersion;
    }

    public AtlasRdbmsSchemaVersion getFromVersion() {
        return fromVersion;
    }

    public AtlasRdbmsSchemaVersion getToVersion() {
        return toVersion;
    }
}
