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
package com.palantir.timestamp.server;

import com.palantir.timestamp.InMemoryTimestampService;
import com.palantir.timestamp.server.config.TimestampServerConfiguration;

import io.dropwizard.Application;
import io.dropwizard.setup.Environment;

public class TimestampServer extends Application<TimestampServerConfiguration> {

    public static void main(String[] args) throws Exception {
        new TimestampServer().run(args);
    }

    @Override
    public void run(TimestampServerConfiguration configuration, Environment environment) throws Exception {
        environment.jersey().register(new InMemoryTimestampService());
    }


}
