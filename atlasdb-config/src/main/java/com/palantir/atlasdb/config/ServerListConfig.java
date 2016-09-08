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
package com.palantir.atlasdb.config;

import java.util.Set;

import javax.validation.constraints.Size;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import com.palantir.remoting.ssl.SslConfiguration;

@JsonDeserialize(as = ImmutableServerListConfig.class)
@JsonSerialize(as = ImmutableServerListConfig.class)
@Value.Immutable
public interface ServerListConfig {
<<<<<<< 7033b8fc57203bf309772ac48101c6126fb91d56

    @Size(min = 1)
    Set<String> servers();

    Optional<SslConfiguration> sslConfiguration();

=======
    @Size(min = 1)
    Set<String> servers();
>>>>>>> merge develop into perf cli branch (#820)
}
