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
package com.palantir.nexus.db.pool.config;

import java.io.File;
import java.util.Map;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.palantir.nexus.db.DBType;
import com.palantir.nexus.db.manager.DBConfigConnectionParameter;

@JsonDeserialize(as = ImmutableConnectionConfig.class)
@JsonSerialize(as = ImmutableConnectionConfig.class)
@Value.Immutable
public abstract class ConnectionConfig {

    @Value.Default
    public int getMinConnections() {
        return 8;
    }

    @Value.Default
    public int getMaxConnections() {
        return 256;
    }

    @Value.Default
    public Integer getMaxConnectionAge() {
        return 1800;
    }

    @Value.Default
    public Integer getMaxIdleTime() {
        return 600;
    }

    @Value.Default
    public Integer getUnreturnedConnectionTimeout() {
        return 0;
    }

    @Value.Default
    public Integer getCheckoutTimeout() {
        return 30000;
    }

    @Value.Default
    public boolean getTwoWaySsl() {
        return false;
    }

    @Value.Default
    public String getConnId() {
        return "atlas";
    }

    @Value.Default
    public int getSocketTimeoutSeconds() {
        return 120;
    }

    @Value.Default
    public int getConnectionTimeoutSeconds() {
        return 45;
    }

    @Value.Derived
    public ImmutableMap<DBConfigConnectionParameter, String> getConnectionParameters() {
        ImmutableMap.Builder<DBConfigConnectionParameter, String> builder =
                ImmutableMap.<DBConfigConnectionParameter, String>builder()
                        .put(DBConfigConnectionParameter.PROTOCOL, getProtocol().getUrlString());
        if (getHost().isPresent()) {
            builder.put(DBConfigConnectionParameter.HOST, getHost().get());
        }
        if (getPort().isPresent()) {
            builder.put(DBConfigConnectionParameter.PORT, Integer.toString(getPort().get()));
        }
        if (getDbName().isPresent()) {
            builder.put(DBConfigConnectionParameter.DBNAME, getDbName().get());
        }
        if (getSid().isPresent()) {
            builder.put(DBConfigConnectionParameter.SID, getSid().get());
        }
        if (getMatchServerDn().isPresent()) {
            builder.put(DBConfigConnectionParameter.MATCH_SERVER_DN, getMatchServerDn().get());
        }
        return builder.build();
    }

    public abstract String getDbLogin();
    public abstract String getDbPassword();
    public abstract DBType getDbType();

    // these are not really optional, but are made so for backwards compatibility
    public abstract Optional<String> getHost();
    public abstract Optional<Integer> getPort();

    public abstract Optional<String> getDbName();
    public abstract Optional<String> getSid();
    public abstract Optional<String> getMatchServerDn();

    @Value.Default
    public ConnectionProtocol getProtocol() {
        return ConnectionProtocol.TCP;
    }

    @Value.Derived
    public String getUrl() {
        String url = getDbType().getDefaultUrl() + getUrlSuffix();
        for (Map.Entry<DBConfigConnectionParameter, String> propEntry : getConnectionParameters().entrySet()) {
            String escapedValue = propEntry.getValue().replaceAll("\\\\","\\\\\\\\");
            url = url.replaceAll("\\{" + propEntry.getKey().name() +"\\}", escapedValue);
        }
        return url;
    }

    @Value.Default
    public String getUrlSuffix() {
        return "";
    }

    @Value.Default
    public String getDriverClass() {
        return getDbType().getDriverName();
    }

    public abstract Optional<String> getKeystorePassword();

    public abstract Optional<String> getKeystorePath();

    public abstract Optional<String> getTruststorePath();

    @Value.Check
    protected final void check() {
        if (getProtocol() == ConnectionProtocol.TCPS) {
            Preconditions.checkArgument(getTruststorePath().isPresent(), "tcps requires a truststore");
            Preconditions.checkArgument(new File(getTruststorePath().get()).exists(), "truststore file not found at %s", getTruststorePath().get());
            if (getTwoWaySsl()) {
                Preconditions.checkArgument(getKeystorePath().isPresent(), "two way ssl requires a keystore");
                Preconditions.checkArgument(new File(getKeystorePath().get()).exists(), "keystore file not found at %s", getKeystorePath().get());
                Preconditions.checkArgument(getKeystorePassword().isPresent(), "two way ssl requires a keystore password");
            }
        } else {
            Preconditions.checkArgument(!getTwoWaySsl(), "two way ssl cannot be enabled without enabling tcps");
        }
    }

}
