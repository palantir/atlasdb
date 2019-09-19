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
package com.palantir.atlasdb.cassandra;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Strings;
import com.palantir.logsafe.Preconditions;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@JsonDeserialize(as = ImmutableCassandraJmxCompactionConfig.class)
@JsonSerialize(as = ImmutableCassandraJmxCompactionConfig.class)
@Value.Immutable
public abstract class CassandraJmxCompactionConfig {
    @Value.Default
    public boolean ssl() {
        return false;
    }

    @Value.Default
    public long rmiTimeoutMillis() {
        return 20000;
    }

    @Value.Default
    public int port() {
        return 7199;
    }

    @Value.Default
    public long compactionTimeoutSeconds() {
        return 30 * 60;
    }

    @Nullable
    public abstract String keystore();

    @Nullable
    public abstract String keystorePassword();

    @Nullable
    public abstract String truststore();

    @Nullable
    public abstract String truststorePassword();

    public abstract String username();

    public abstract String password();

    @Value.Check
    protected final void check() {
        if (!ssl()) {
            return;
        }

        Preconditions.checkState(!Strings.isNullOrEmpty(keystore()), "keystore must be specified");
        Preconditions.checkState(!Strings.isNullOrEmpty(keystorePassword()), "keystorePassword must be specified");
        Preconditions.checkState(!Strings.isNullOrEmpty(truststore()), "truststore must be specified");
        Preconditions.checkState(!Strings.isNullOrEmpty(truststorePassword()), "truststorePassword must be specified");
    }
}
