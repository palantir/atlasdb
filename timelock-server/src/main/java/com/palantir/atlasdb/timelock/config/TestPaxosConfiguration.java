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
package com.palantir.atlasdb.timelock.config;


import java.io.File;

import org.immutables.value.Value;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.palantir.atlasdb.timelock.TimeLockServer;
import com.palantir.atlasdb.timelock.paxos.PaxosTimeLockServer;
import com.palantir.atlasdb.timelock.paxos.TestPaxosTimeLockServer;
import com.palantir.remoting.ssl.SslConfiguration;

import io.dropwizard.setup.Environment;

    @JsonSerialize(as = ImmutablePaxosConfiguration.class)
    @JsonDeserialize(as = ImmutablePaxosConfiguration.class)
    public abstract class TestPaxosConfiguration extends PaxosConfiguration {
        @Override
        public TimeLockServer createServerImpl(Environment environment) {
            return new TestPaxosTimeLockServer(this, environment);
        }
    }

