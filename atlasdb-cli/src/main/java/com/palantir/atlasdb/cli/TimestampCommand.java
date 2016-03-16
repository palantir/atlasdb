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
package com.palantir.atlasdb.cli;

import com.palantir.atlasdb.cli.api.OldAtlasDbServices;
import com.palantir.atlasdb.cli.api.SingleBackendCommand;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "timestamp", description = "Get timestamp information")
public class TimestampCommand extends SingleBackendCommand {
    
    @Option(name = {"-f", "--fresh"},
            description = "Get a fresh timestamp")
    private boolean fresh;
    
    @Option(name = {"-i", "--immutable"},
    		description = "Get the current immutable timestamp")
    private boolean immutable;
    
	@Override
	protected int execute(OldAtlasDbServices services) {
		long latestTimestamp = services.getTimestampService().getFreshTimestamp();

        if (fresh || !(fresh || immutable)) {
            System.out.println("Current timestamp is: " + latestTimestamp); // (authorized)
        }

        if (immutable) {
        	long immutableTimestamp = services.getTransactionManager().getImmutableTimestamp();
            System.out.println("Current immutable timestamp is: " + immutableTimestamp); // (authorized)
        }

        return 0;
	}
}
