/**
 * Copyright 2017 Palantir Technologies
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
package com.palantir.atlasdb.timelock;

import java.util.List;
import java.util.Set;

import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.palantir.remoting.ssl.SslConfiguration;

@JsonDeserialize(as = ImmutableTesterConfiguration.class)
@JsonSerialize(as = ImmutableTesterConfiguration.class)
@Value.Immutable
@Value.Style(visibility = ImplementationVisibility.PACKAGE)
public abstract class TesterConfiguration {

    @Value.Default
    public int numClients() {
        return 1;
    }

    @Value.Default
    public int numThreads() {
        return 10;
    }

    public abstract List<String> namespaces();

    //queriesPerSecond for each client
    public abstract List<Integer> queriesPerSecond();

    public abstract Set<String> paths();

    public abstract SslConfiguration sslConfiguration();

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends ImmutableTesterConfiguration.Builder {}

}
