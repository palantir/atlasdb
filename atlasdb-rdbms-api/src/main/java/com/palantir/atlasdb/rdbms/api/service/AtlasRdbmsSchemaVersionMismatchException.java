package com.palantir.atlasdb.rdbms.api.service;

/**
 * An exception that is thrown when the schema version of
 * an AtlasRdbms does not match the version a client expects.
 * @author mharris
 *
 */
public class AtlasRdbmsSchemaVersionMismatchException extends Exception {
    private static final long serialVersionUID = 1L;

    private final AtlasRdbmsSchemaVersion expectedVersion;
    private final AtlasRdbmsSchemaVersion actualVersion;

    public AtlasRdbmsSchemaVersionMismatchException(AtlasRdbmsSchemaVersion expectedVersion,
                                            AtlasRdbmsSchemaVersion actualVersion) {
        super("Expected version: " + expectedVersion + ", but was: " + actualVersion);
        this.expectedVersion = expectedVersion;
        this.actualVersion = actualVersion;
    }

    public AtlasRdbmsSchemaVersion getExpectedVersion() {
        return expectedVersion;
    }

    public AtlasRdbmsSchemaVersion getActualVersion() {
        return actualVersion;
    }
}
