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
package com.palantir.atlasdb.cli.command.timestamp;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cli.command.SingleBackendCommand;
import com.palantir.atlasdb.cli.services.AtlasDbServices;

import io.airlift.airline.Option;
import io.airlift.airline.OptionType;

public abstract class AbstractTimestampCommand extends SingleBackendCommand {

    private static final Logger log = LoggerFactory.getLogger(AbstractTimestampCommand.class);

    @Option(name = {"-f", "--file"},
            title = "TIMESTAMP_FILE",
            type = OptionType.GROUP,
            description = "Write/Read the timestamp for this command via the specified file.")
    private File file;
    
    @Option(name = {"-t", "--timestamp"},
            title = "TIMESTAMP",
            type = OptionType.GROUP,
            description = "The timestamp for to use for this command")
    Long timestamp;

    protected abstract boolean requireTimestamp();
    
    protected abstract int executeTimestampCommand(AtlasDbServices services);
    
    @Override
    public int execute(AtlasDbServices services) {
        initialize();
        return executeTimestampCommand(services);
    }
    
    private void initialize() {
        if (!requireTimestamp() && timestamp != null) {
            throw new IllegalArgumentException("This command does not require an input timestamp but you specified one.");
        } else if (requireTimestamp() && ((timestamp == null && file == null) || (timestamp != null && file != null))) {
            throw new IllegalArgumentException("This command requires an input timestamp, "
                    + "so you must specify one and only one (either directly or via a file).");
        } else if (requireTimestamp() && file != null) {
            timestamp = readTimestampFromFile();
        }
    }

	private long readTimestampFromFile() {
        String timestamp;
        try {
            timestamp = StringUtils.strip(Iterables.getOnlyElement(Files.readAllLines(file.toPath())));
        } catch (IOException e) {
            log.error("IOException thrown reading timestamp from file: {}", file.getPath());
            throw Throwables.propagate(e);
        }
        return Long.parseLong(timestamp);
    }

	private void writeTimestampToFile() {
        Set<String> lines = Sets.newHashSet(Long.toString(timestamp));
        try {
            Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.error("IOException thrown writing timestamp to file: {}", file.getPath());
            Throwables.propagate(e);
        }
	}

	protected void writeTimestampToFileIfSpecified() {
	    if (file != null) {
	        writeTimestampToFile();
	    }
	}
}
