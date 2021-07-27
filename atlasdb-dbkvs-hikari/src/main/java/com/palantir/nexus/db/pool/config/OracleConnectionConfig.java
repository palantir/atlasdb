/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.nexus.db.pool.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import com.palantir.nexus.db.DBType;
import java.io.File;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableOracleConnectionConfig.class)
@JsonSerialize(as = ImmutableOracleConnectionConfig.class)
@JsonTypeName(OracleConnectionConfig.TYPE)
@Value.Immutable
public abstract class OracleConnectionConfig extends ConnectionConfig {
    private static final SafeLogger log = SafeLoggerFactory.get(OracleConnectionConfig.class);

    public static final String TYPE = "oracle";

    public abstract String getHost();

    public abstract int getPort();

    @Override
    @Value.Default
    public String getUrl() {
        if (getServerDn().isPresent()) {
            return String.format(
                    "jdbc:oracle:thin:@(DESCRIPTION="
                            + "(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))"
                            + "(CONNECT_DATA=(%s))"
                            + "(SECURITY=(SSL_SERVER_CERT_DN=\"%s\")))",
                    getProtocol().getUrlString(),
                    getHost(),
                    getPort(),
                    connectionDataString(),
                    getServerDn().get());
        } else {
            return String.format(
                    "jdbc:oracle:thin:@(DESCRIPTION="
                            + "(ADDRESS=(PROTOCOL=%s)(HOST=%s)(PORT=%s))"
                            + "(CONNECT_DATA=(%s)))",
                    getProtocol().getUrlString(), getHost(), getPort(), connectionDataString());
        }
    }

    @Override
    @Value.Derived
    @JsonIgnore
    public Optional<String> namespace() {
        if (getSid().isPresent()) {
            return getSid();
        }
        return serviceNameConfiguration().map(ServiceNameConfiguration::namespaceOverride);
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

    public abstract Optional<String> getSid();

    public abstract Optional<ServiceNameConfiguration> serviceNameConfiguration();

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
        return ConnectionProtocol.TCP;
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
        props.setProperty(
                "oracle.jdbc.ReadTimeout", Long.toString(TimeUnit.SECONDS.toMillis(getSocketTimeoutSeconds())));

        props.setProperty(
                "oracle.net.CONNECT_TIMEOUT", Long.toString(TimeUnit.SECONDS.toMillis(getConnectionTimeoutSeconds())));

        props.setProperty("oracle.jdbc.maxCachedBufferSize", "100000");

        if (getProtocol() == ConnectionProtocol.TCPS) {
            // Create the truststore
            File clientTruststore = new File(getTruststorePath().get());
            props.setProperty("javax.net.ssl.trustStore", clientTruststore.getAbsolutePath());
            props.setProperty(
                    "javax.net.ssl.trustStorePassword", getTruststorePassword().get());

            // Enable server domain matching
            if (getMatchServerDn()) {
                props.setProperty("oracle.net.ssl_server_dn_match", "true");
            }

            // Enable client SSL certificate support. "two-way" SSL in Oracle parlance.
            if (getTwoWaySsl()) {
                Preconditions.checkArgument(getKeystorePath().isPresent());
                Preconditions.checkArgument(getKeystorePassword().isPresent());
                props.setProperty("javax.net.ssl.keyStore", getKeystorePath().get());
                props.setProperty(
                        "javax.net.ssl.keyStorePassword", getKeystorePassword().get());
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
        Preconditions.checkArgument(
                getSid().isPresent() ^ serviceNameConfiguration().isPresent(),
                "Exactly one of sid and serviceNameConfiguration should be provided.");

        if (getProtocol() == ConnectionProtocol.TCPS) {
            Preconditions.checkArgument(
                    getTruststorePath().isPresent(), "ConnectionProtocol.TCPS requires a truststore");
            com.google.common.base.Preconditions.checkArgument(
                    new File(getTruststorePath().get()).exists(),
                    "truststore file not found at %s",
                    getTruststorePath().get());
            Preconditions.checkArgument(
                    getTruststorePassword().isPresent(), "ConnectionProtocol.TCPS requires a truststore password");
            if (getTwoWaySsl()) {
                Preconditions.checkArgument(getKeystorePath().isPresent(), "two way ssl requires a keystore");
                com.google.common.base.Preconditions.checkArgument(
                        new File(getKeystorePath().get()).exists(),
                        "keystore file not found at %s",
                        getKeystorePath().get());
                Preconditions.checkArgument(
                        getKeystorePassword().isPresent(), "two way ssl requires a keystore password");
            }
            if (!getServerDn().isPresent()) {
                Preconditions.checkArgument(!getMatchServerDn(), "cannot force match server dn without a server dn");
            }
        } else {
            Preconditions.checkArgument(
                    !getTwoWaySsl(), "two way ssl cannot be enabled without enabling ConnectionProtocol.TCPS");
            if (getServerDn().isPresent()
                    || getTruststorePath().isPresent()
                    || getTruststorePassword().isPresent()
                    || getKeystorePath().isPresent()
                    || getKeystorePassword().isPresent()) {
                log.debug("Your Oracle config is not set to use ConnectionProtocol.TCPS, but some security settings"
                        + " have been set. These settings are currently being ignored - please verify that you are"
                        + " OK with this.");
            }
        }
    }

    private String connectionDataString() {
        if (getSid().isPresent()) {
            return "SID=" + getSid().get();
        }

        if (serviceNameConfiguration().isPresent()) {
            return "SERVICE_NAME=" + serviceNameConfiguration().get().serviceName();
        }

        throw new SafeIllegalArgumentException(
                "Both the sid and serviceNameConfiguration are absent." + " One needs to be specified.");
    }

    public static class Builder extends ImmutableOracleConnectionConfig.Builder {}

    @JsonDeserialize(as = ImmutableServiceNameConfiguration.class)
    @JsonSerialize(as = ImmutableServiceNameConfiguration.class)
    @Value.Immutable
    public interface ServiceNameConfiguration {
        String serviceName();

        String namespaceOverride();

        class Builder extends ImmutableServiceNameConfiguration.Builder {}
    }
}
