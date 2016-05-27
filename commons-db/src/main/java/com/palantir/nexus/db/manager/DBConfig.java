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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.pool.PoolType;

public interface DBConfig {

    public int getInitialConnections();

    public int getMinConnections();

    public int getMaxConnections();

    public Integer getMaxConnectionAge();

    public Integer getMaxIdleTime();

    public Integer getUnreturnedConnectionTimeout();

    public Boolean getDebugUnreturnedConnectionStackTraces();

    @VisibleForTesting
    public void setCheckoutTimeout(int timeout);

    public Integer getCheckoutTimeout();

    public void setTwoWaySsl(boolean twoWaySsl);
    public boolean getTwoWaySsl();

    public Integer getSocketTimeoutSeconds();

    public Integer getConnectTimeoutSeconds();

    /**
     * Returns the connection parameters for this db config.
     *
     * @return
     */
    public ImmutableMap<DBConfigConnectionParameter, String> getConnectionParameters();

    /**
     * Adds a connection parameter to this config.
     *
     * @param name
     * @param value
     */
    public void addConnectionParameter(DBConfigConnectionParameter name, String value);

    /**
     * Returns the named connection parameter value for a named parameter.
     *
     * @param name
     * @return
     */
    public String getConnectionParameter(DBConfigConnectionParameter name);

    /**
     * Returns the JDBC connection URL for this DBConfig.
     *
     * @return
     */
    public String getUrl();

    public void setUrlSuffix(String urlSuffix);

    public String getUrlSuffix();

    public String getConnId();

    public String getDbLogin();

    public String getDbDecryptedPassword();

    public DBType getType();

    public String getDriverClass();

    public PoolType getPoolType();

    public int getNumRetryAttempts();

    /**
     * Returns the path of the file used to configure this if it
     * was configured from a file, otherwise null.
     */
    public String getConfigPath();

    public DBConfig copy();

    public void setManateeConnectString(String manateeConnectString);
    public String getManateeConnectString();
    public void setManateeShard(String manateeShard);
    public String getManateeShard();

    /**
     * Get an identifying string for the database.  Generally when connecting
     * to the same DB these should be equal and when connecting to different
     * DBs they should not.
     *
     * Things to do with these Strings: compare them as if they were opaque to
     * try to decide if a DB is the "same one".
     *
     * Things to not ever do with these Strings: anything else.
     */
    public String getDatabaseIdentifier();
}
