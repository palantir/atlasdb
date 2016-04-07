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
