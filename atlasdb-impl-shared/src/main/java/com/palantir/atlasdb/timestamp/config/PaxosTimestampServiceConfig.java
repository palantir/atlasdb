/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.timestamp.config;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.service.AutoService;
import com.palantir.atlasdb.spi.TimestampServiceConfig;
import com.palantir.paxos.config.PaxosProposerConfig;

@AutoService(TimestampServiceConfig.class)
@JsonDeserialize(as = ImmutablePaxosTimestampServiceConfig.class)
@JsonSerialize(as = ImmutablePaxosTimestampServiceConfig.class)
@JsonTypeName(PaxosTimestampServiceConfig.TYPE)
@Value.Immutable
public abstract class PaxosTimestampServiceConfig implements TimestampServiceConfig {
    public static final String TYPE = "paxos";

    @Override
    public final String type() {
        return TYPE;
    }

    @Value.Default
    public String getLogName() {
        return "timestamp";
    }

    public abstract PaxosProposerConfig proposer();

}
