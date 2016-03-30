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
package com.palantir.atlasdb.timelock.server;

import com.palantir.atlasdb.timelock.server.modules.ConfigModule;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

public class TimeLockServer extends Application<TimeLockConfiguration> {

    public static void main(String[] args) throws Exception {
        new TimeLockServer().run(args);
    }

    @Override
    public void run(TimeLockConfiguration config, final Environment environment) throws Exception {
        ServerEndpoints serverEndpoints = DaggerServerEndpoints.builder()
                .configModule(new ConfigModule(config))
                .build();

        serverEndpoints.forEach(environment.jersey()::register);
    }

}
