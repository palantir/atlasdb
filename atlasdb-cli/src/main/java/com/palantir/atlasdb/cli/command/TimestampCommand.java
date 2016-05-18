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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;

import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cleaner.KeyValueServicePuncherStore;
import com.palantir.atlasdb.cli.services.AtlasDbServices;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "timestamp", description = "Fetches a timestamp. By default"
        + " this will be a fresh timestamp unless otherwise specified.")
public class TimestampCommand extends SingleBackendCommand {

    @Option(name = {"-i", "--immutable"},
    		description = "Get the current immutable timestamp, instead of a fresh one.")
    private boolean immutable;
    
    @Option(name = {"-d", "--date-time"},
            description = "Return the earliest approximate wall clock datetime at which the chosen timestamp" +
                    " could have been used in a transaction.")
    private boolean dateTime;

    @Option(name = {"-f", "--file"},
            title = "TIMESTAMP_FILE",
            description = "Write the timestamp returned by this command to the specified file.")
    private File file;
    
    private static final String IMMUTABLE_STRING = "Immutable";
    private static final String FRESH_STRING = "Fresh";
    
	@Override
	public int execute(AtlasDbServices services) {
	    long timestamp;
	    String name;
	    if (immutable) {
    	    timestamp = services.getTransactionManager().getImmutableTimestamp();
    	    name = IMMUTABLE_STRING;
	    } else {
	        timestamp = services.getTimestampService().getFreshTimestamp();
	        name = FRESH_STRING;
	    }
	    System.out.printf("The %s timestamp is: %d\n", name, timestamp);

	    String stringTime = null;
        if (dateTime) {
            long timeMillis = KeyValueServicePuncherStore.getMillisForTimestamp(
                    services.getKeyValueService(), timestamp);
            DateTime dt = new DateTime(timeMillis);
            stringTime = ISODateTimeFormat.dateTimeNoMillis().print(dt);
            System.out.printf("Wall clock datetime of %s timestamp is: %s\n", name, stringTime);
        }

        if (file != null) {
            Set<String> lines = Sets.newHashSet(Long.toString(timestamp));
            if (dateTime) {
                lines.add(stringTime);
            }
            try {
                Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
            } catch (IOException e) {
                System.err.printf("IOException thrown writing %s timestamp to file: %s\n", name, file.getPath());
                Throwables.propagate(e);
            }
        }

        System.out.println("Timestamp command completed succesfully.");
        return 0;
	}
}
