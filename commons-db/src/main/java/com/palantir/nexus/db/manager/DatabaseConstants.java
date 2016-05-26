/**
 * Copyright 2015 Palantir Technologies
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
 */
package com.palantir.nexus.db.manager;

public class DatabaseConstants {
    // db prefs constants
    public static final String DB_PREFS_HOST = "DB_PREFS_HOST"; //$NON-NLS-1$
    public static final String DB_PREFS_PORT = "DB_PREFS_PORT"; //$NON-NLS-1$
    public static final String DB_PREFS_USERNAME = "DB_PREFS_USERNAME"; //$NON-NLS-1$
    public static final String DB_PREFS_PASSWORD = "DB_PREFS_PASSWORD"; //$NON-NLS-1$
    public static final String DB_PREFS_SID = "DB_PREFS_SID"; //$NON-NLS-1$
    public static final String DB_PREFS_MATCH_SERVER_DN = "DB_PREFS_MATCH_SERVER_DN"; //$NON-NLS-1$
    public static final String DB_PREFS_PROTOCOL = "DB_PREFS_PROTOCOL"; //$NON-NLS-1$
    public static final String DB_PREFS_DBNAME = "DB_PREFS_DBNAME"; //$NON-NLS-1$ //$NON_NLS-1$
    public static final String DB_PREFS_TYPE = "DB_PREFS_TYPE"; //$NON-NLS-1$

    public static final String DB_PREFS_POOL_DRIVER = "DB_PREFS_POOL_DRIVER";

    public static final String DB_PREFS_MANATEE_SHARD = "DB_PREFS_MANATEE_SHARD";
    public static final String DB_PREFS_MANATEE_ZK_URL = "DB_PREFS_MANATEE_ZK_URL";

    public static final String DB_PREFS_MIN_CONNS = "DB_PREFS_MIN_CONNS"; //$NON-NLS-1$
    public static final String DB_PREFS_MAX_CONNS = "DB_PREFS_MAX_CONNS"; //$NON-NLS-1$
    public static final String DB_PREFS_INITIAL_CONNS = "DB_PREFS_INITIAL_CONNS"; //$NON-NLS-1$
    public static final String DB_PREFS_MAX_CONNECTION_AGE = "DB_PREFS_MAX_CONNECTION_AGE"; //$NON-NLS-1$
    public static final String DB_PREFS_MAX_IDLE_TIME = "DB_PREFS_MAX_IDLE_TIME"; //$NON-NLS-1$
    public static final String DB_PREFS_UNRETURNED_CONNECTION_TIMEOUT = "DB_PREFS_UNRETURNED_CONNECTION_TIMEOUT"; //$NON-NLS-1$
    public static final String DB_PREFS_DEBUG_UNRETURNED_CONNECTION_STACK_TRACES = "DB_PREFS_DEBUG_UNRETURNED_CONNECTION_STACK_TRACES"; //$NON-NLS-1$
    public static final String DB_PREFS_CHECKOUT_TIMEOUT = "DB_PREFS_CHECKOUT_TIMEOUT"; //$NON-NLS-1$
    public static final String DB_PREFS_TWO_WAY_SSL = "DB_PREFS_TWO_WAY_SSL"; //$NON-NLS-1$
    public static final String DB_PREFS_SOCKET_TIMEOUT_SECONDS = "DB_PREFS_SOCKET_TIMEOUT_SECONDS"; //$NON-NLS-1$
    public static final String DB_PREFS_CONNECT_TIMEOUT_SECONDS = "DB_PREFS_CONNECT_TIMEOUT_SECONDS"; //$NON-NLS-1$

    // Atlas
    public static final String DB_PREFS_ATLAS_TABLES = "DB_PREFS_ATLAS_TABLES"; //$NON-NLS-1$
    public static final String DB_PREFS_CASSANDRA_KEYSPACE = "DB_PREFS_CASSANDRA_KEYSPACE"; //$NON-NLS-1$
    public static final String DB_PREFS_CASSANDRA_USE_SSL = "DB_PREFS_CASSANDRA_USE_SSL"; //$NON-NLS-1$
    public static final String DB_PREFS_CASSANDRA_SERVER_ADDRESS_LIST = "DB_PREFS_CASSANDRA_SERVER_ADDRESS_LIST"; //$NON-NLS-1$
    public static final String DB_PREFS_USE_ATLAS_POSTGRES_TEMP_TABLES = "DB_PREFS_USE_ATLAS_POSTGRES_TEMP_TABLES"; //$NON-NLS-1$

    /**
     * Specify a custom JDBC URL. The URL is used as-is, without any sort of
     * variable substitution.
     */
    public static final String DB_PREFS_CUSTOM_JDBC_URL = "DB_PREFS_CUSTOM_JDBC_URL"; //$NON-NLS-1$
    public static final String DB_PREFS_CUSTOM_JDBC_DRIVER_CLASS = "DB_PREFS_CUSTOM_JDBC_DRIVER_CLASS";

    public static final String DEFAULT_DB_CONNECTION_PARAM_PROTOCOL = "tcp"; //$NON-NLS-1$
    public static final String DB_ORACLE_SECURITY_SUFFIX = "(SECURITY=(SSL_SERVER_CERT_DN=\"{MATCH_SERVER_DN}\")))"; //$NON-NLS-1$
    public static final String DB_ORACLE_NO_SECURITY_SUFFIX = ")"; //$NON-NLS-1$
    public static final String DEFAULT_PREF_FILE = "dispatch.prefs";
}
