/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.atlasdb.cli.command.timestamp;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.cli.command.SingleBackendCommand;
import com.palantir.atlasdb.cli.output.OutputPrinter;
import com.palantir.atlasdb.services.AtlasDbServices;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

public abstract class AbstractTimestampCommand extends SingleBackendCommand {

    private static final OutputPrinter printer = new OutputPrinter(
            LoggerFactory.getLogger(AbstractTimestampCommand.class));

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
            throw new SafeIllegalArgumentException("This command does not require an input timestamp"
                    + " but you specified one.");
        } else if (requireTimestamp() && ((timestamp == null && file == null) || (timestamp != null && file != null))) {
            throw new SafeIllegalArgumentException("This command requires an input timestamp, "
                    + "so you must specify one and only one (either directly or via a file).");
        } else if (requireTimestamp() && file != null) {
            timestamp = readTimestampFromFile();
        }
    }

    private long readTimestampFromFile() {
        String timestampString;
        try {
            timestampString = StringUtils.strip(Iterables.getOnlyElement(Files.readAllLines(file.toPath())));
        } catch (IOException e) {
            printer.error("IOException thrown reading timestamp from file: {}",
                    SafeArg.of("file path", file.getPath()));
            throw Throwables.propagate(e);
        }
        return Long.parseLong(timestampString);
    }

    private void writeTimestampToFile() {
        Set<String> lines = Sets.newHashSet(Long.toString(timestamp));
        try {
            if (file.getParentFile() != null && !file.getParentFile().exists()) {
                Files.createDirectories(file.getParentFile().toPath());
            }
            Files.write(file.toPath(), lines, StandardCharsets.UTF_8);
        } catch (IOException e) {
            printer.error("IOException thrown writing timestamp to file: {}", SafeArg.of("file path", file.getPath()));
            Throwables.propagate(e);
        }
    }

    protected void writeTimestampToFileIfSpecified() {
        if (file != null) {
            writeTimestampToFile();
        }
    }
}
