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
package com.palantir.atlasdb.performance.cli.command;

import java.util.concurrent.Callable;

import com.palantir.atlasdb.performance.cli.backend.PhysicalStore;
import com.palantir.atlasdb.performance.cli.backend.PhysicalStoreType;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "run", description = "Run tests")
public class RunTestsCommand implements Callable<Integer> {

    @Option(name = {"-b", "--backend"},
            title = "PHYSICAL STORE",
            description = "underyling physical store (e.g. POSTGRES)",
            required = true)
    private PhysicalStoreType type;

    @Override
    public Integer call() throws Exception {
        PhysicalStore.create(type).connect();
        return 0;
    }
}
