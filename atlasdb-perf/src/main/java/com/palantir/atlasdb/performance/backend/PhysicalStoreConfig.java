package com.palantir.atlasdb.performance.backend;

/**
 * Class for storing all configuration constants for the each physical store.
 *
 * @author mwakerman
 */
class PhysicalStoreConfig {

    //================================================================================================================
    // POSTGRES
    //================================================================================================================

    static final String POSTGRES_DB_NAME             = "atlas";
    static final int    POSTGRES_PORT_NUMBER         = 5432;
    static final String POSTGRES_USER_LOGIN          = "palantir";
    static final String POSTGRES_USER_PASSWORD       = "palantir";
    static final String POSTGRES_DOCKER_COMPOSE_PATH = "/postgres-docker-compose.yml";
    static final String POSTGRES_DOCKER_LOGS_DIR     = "container-logs";

}
