package com.palantir.nexus.db.sql;

/**
 * Enumerates methods for clearing SQL tables.
 */
enum ClearStyle {
    /**
     * Clear tables using TRUNCATE. Experts-only.
     *
     * NOTE: the semantics for executing a TRUNCATE TABLE statement within a
     * transaction are vendor-specific. You must exercise caution when using
     * this option!
     */
    TRUNCATE,

    /**
     * Clear tables using DELETE. Always safe.
     */
    DELETE
}
