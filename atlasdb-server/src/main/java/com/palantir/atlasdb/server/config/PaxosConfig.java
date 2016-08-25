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
package com.palantir.atlasdb.server.config;

import java.io.File;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;

@JsonSerialize(as = ImmutablePaxosConfig.class)
@JsonDeserialize(as = ImmutablePaxosConfig.class)
@Value.Immutable
public abstract class PaxosConfig {
    public static final PaxosConfig DEFAULT = ImmutablePaxosConfig.builder().build();

    @Value.Default
    public File learnerLogDir() {
        return new File("var/data/paxos/learner");
    }

    @Value.Default
    public File acceptorLogDir() {
        return new File("var/data/paxos/acceptor");
    }

    @Value.Check
    protected void check() {
        Preconditions.checkArgument(learnerLogDir().exists() || learnerLogDir().mkdirs(),
                "Learner log directory '%s' does not exist and cannot be created.", learnerLogDir());
        Preconditions.checkArgument(acceptorLogDir().exists() || acceptorLogDir().mkdirs(),
                "Acceptor log directory '%s' does not exist and cannot be created.", acceptorLogDir());
    }
}
