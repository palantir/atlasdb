/**
 * Copyright 2016 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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
