/*
 * Copyright 2016 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.nexus.db.pool.config;

import static com.palantir.nexus.db.pool.config.ConnectionProtocol.TCP;
import static com.palantir.nexus.db.pool.config.ConnectionProtocol.TCPS;

import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.palantir.nexus.db.DBType;

@JsonDeserialize(as = ImmutableOracleConnectionConfig.class)
@JsonSerialize(as = ImmutableOracleConnectionConfig.class)
@JsonTypeName(OracleConnectionConfig.TYPE)
@Value.Immutable
public abstract class OracleConnectionConfig extends ConnectionConfig {

    public static final String TYPE = "oracle";

    public abstract String getHost();
    public abstract int getPort();

    @Override
    @Value.Derived
    @JsonIgnore
    public String getUrl() {
        if (getServerDn().isPresent()) {
            return String.format("jdbc:oracle:thin:@(DESCRIPTION=" +
                            "(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))" +
                            "(CONNECT_DATA=(SID=%s))" +
                            "(SECURITY=(SSL_SERVER_CERT_DN=\"%s\")))",
                    getProtocol().getUrlString(), getHost(), getPort(), getSid(), getServerDn().get());
        } else {
            return String.format("jdbc:oracle:thin:@(DESCRIPTION=" +
                            "(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))" +
                            "(CONNECT_DATA=(SID=%s)))",
                    getProtocol().getUrlString(), getHost(), getPort(), getSid());
        }
    }

    @Override
    @Value.Default
    public String getDriverClass() {
        return "oracle.jdbc.driver.OracleDriver";
    }

    @Override
    @Value.Default
    public String getTestQuery() {
        return "SELECT 1 FROM dual";
    }

    public abstract String getSid();

    public abstract Optional<String> getServerDn();

    @Value.Default
    public boolean getMatchServerDn() {
        return getServerDn().isPresent();
    }

    @Value.Default
    public boolean getTwoWaySsl() {
        return false;
    }

    /**
     * Set arbitrary additional connection parameters.
     * See https://docs.oracle.com/cd/E11882_01/appdev.112/e13995/oracle/jdbc/OracleDriver.html
     */
    @Value.Default
    public Map<String, String> getConnectionParameters() {
        return ImmutableMap.of();
    }

    @Value.Default
    public ConnectionProtocol getProtocol() {
        return TCP;
    }

    public abstract Optional<String> getKeystorePassword();
    public abstract Optional<String> getKeystorePath();

    public abstract Optional<String> getTruststorePassword();
    public abstract Optional<String> getTruststorePath();

    @Override
    @Value.Default
    @Value.Auxiliary
    public Properties getHikariProperties() {
        Properties props = new Properties();
        props.putAll(getConnectionParameters());

        props.setProperty("user", getDbLogin());
        props.setProperty("password", getDbPassword().unmasked());

        props.setProperty("oracle.net.keepAlive", "true");
        props.setProperty("oracle.jdbc.ReadTimeout", Long.toString(TimeUnit.SECONDS.toMillis(getSocketTimeoutSeconds())));

        props.setProperty("oracle.net.CONNECT_TIMEOUT", Long.toString(TimeUnit.SECONDS.toMillis(getConnectionTimeoutSeconds())));

        props.setProperty("oracle.jdbc.maxCachedBufferSize", "100000");

        if (getProtocol() == TCPS) {
            // Create the truststore
            File clientTrustore = new File(getTruststorePath().get());
            props.setProperty("javax.net.ssl.trustStore", clientTrustore.getAbsolutePath());
            props.setProperty("javax.net.ssl.trustStorePassword", "ptclient");

            // Enable server domain matching
            if (getMatchServerDn()) {
                props.setProperty("oracle.net.ssl_server_dn_match", "true");
            }

            // Enable client SSL certificate support. "two-way" SSL in Oracle parlance.
            if (getTwoWaySsl()) {
                Preconditions.checkArgument(getKeystorePath().isPresent());
                Preconditions.checkArgument(getKeystorePassword().isPresent());
                props.setProperty("javax.net.ssl.keyStore", getKeystorePath().get());
                props.setProperty("javax.net.ssl.keyStorePassword", getKeystorePassword().get());
            }
        }

        return props;
    }

    @Override
    @Value.Derived
    public DBType getDbType() {
        return DBType.ORACLE;
    }

    @Override
    public final String type() {
        return TYPE;
    }

    @Value.Check
    protected final void check() {
        if (getProtocol() == TCPS) {
            Preconditions.checkArgument(getTruststorePath().isPresent(), "tcps requires a truststore");
            Preconditions.checkArgument(new File(getTruststorePath().get()).exists(), "truststore file not found at %s", getTruststorePath().get());
            Preconditions.checkArgument(getTruststorePassword().isPresent(), "tcps requires a truststore password");
            if (getTwoWaySsl()) {
                Preconditions.checkArgument(getKeystorePath().isPresent(), "two way ssl requires a keystore");
                Preconditions.checkArgument(new File(getKeystorePath().get()).exists(), "keystore file not found at %s", getKeystorePath().get());
                Preconditions.checkArgument(getKeystorePassword().isPresent(), "two way ssl requires a keystore password");
            }
            if (!getServerDn().isPresent()) {
                Preconditions.checkArgument(!getMatchServerDn(), "cannot force match server dn without a server dn");
            }
        } else {
            Preconditions.checkArgument(!getTwoWaySsl(), "two way ssl cannot be enabled without enabling tcps");
            Preconditions.checkArgument(!getServerDn().isPresent(), "a server dn cannot be given without enabling tcps");
            Preconditions.checkArgument(!getTruststorePath().isPresent(), "a truststore path cannot be given without enabling tcps");
            Preconditions.checkArgument(!getTruststorePassword().isPresent(), "a truststore password cannot be given without enabling tcps");
            Preconditions.checkArgument(!getKeystorePath().isPresent(), "a keystore file cannot be given without enabling tcps");
            Preconditions.checkArgument(!getKeystorePassword().isPresent(), "a keystore password without enabling tcps");
        }
    }
}
