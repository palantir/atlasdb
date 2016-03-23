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
package com.palantir.atlasdb.cli.command;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.cli.services.AtlasDbServices;

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
    
    @Option(name = {"-d", "--date-time"},
            description = "Return the earliest real point in time at which the immutable timestamp" +
                    " could have been used in a transaction.  Requires --immutable to be set")
    private boolean dateTime;
    
	@Override
	public int execute(AtlasDbServices services) {
	    if(dateTime && !immutable) {
	        System.err.println("You can't use --date-time without also using --immutable");
	        return 1;
	    }
	    
	    long latestTimestamp = services.getTimestampService().getFreshTimestamp();

        if (fresh || !(fresh || immutable)) {
            System.out.println("Fresh timestamp is: " + latestTimestamp);
        }

        if (immutable) {
        	long immutableTimestamp = services.getTransactionManager().getImmutableTimestamp();
            System.out.println("Current immutable timestamp is: " + immutableTimestamp);
            
            if (dateTime) {
                long timeMillis = KeyValueServicePuncherStore.getMillisForTimestamp(
                        services.getKeyValueService(), immutableTimestamp);
                DateTime dt = new DateTime(timeMillis);
                String stringTime = ISODateTimeFormat.dateTimeNoMillis().print(dt);
                System.out.printf("Real date time of immutable timestamp is: " + stringTime);
            }
        }

        return 0;
	}
}
