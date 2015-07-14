package com.palantir.atlasdb.rdbms.api.service;

/**
 * Defines server constants in a central location.
 * @author mharris
 *
 */
public final class AtlasRdbmsConstants {

    /**
     * The system property representing the major version of the RDBMS schema
     */
    public static final String PROPERTY_DB_SCHEMA_VERSION = "DB_SCHEMA_VERSION";

    /**
     * The system property representing the hotfix version of the RDBMS schema
     */
    public static final String PROPERTY_DB_SCHEMA_HOTFIX_VERSION = "DB_SCHEMA_HOTFIX_VERSION";

    /**
     * Private Constructor to disallow instantiation.
     */
    private AtlasRdbmsConstants() {
        //
    }
}
