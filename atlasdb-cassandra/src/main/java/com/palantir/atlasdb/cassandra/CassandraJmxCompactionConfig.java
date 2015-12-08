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
package com.palantir.atlasdb.cassandra;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonDeserialize(as = ImmutableCassandraJmxCompactionConfig.class)
@JsonSerialize(as = ImmutableCassandraJmxCompactionConfig.class)
@Value.Immutable
public abstract class CassandraJmxCompactionConfig {

    @Value.Default
    public boolean ssl() {
        return false;
    }

    @Value.Default
    public long jmxRmiTimeoutMillis() {
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

    public abstract String keystore();

    public abstract String keystorePassword();

    public abstract String truststore();

    public abstract String truststorePassword();

    public abstract String username();

    public abstract String password();

}
