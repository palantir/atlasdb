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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.palantir.atlasdb.cli.services.AtlasDbServices;
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

import io.airlift.airline.Command;
import io.airlift.airline.Option;

@Command(name = "fast-forward", description = "Fast forward the stored upper limit of a persistent timestamp"
        + " service to the specified timestamp.  Is used in the restore process to ensure that all future timestamps used are"
        + " explicity greater than any that may have been used to write data to the KVS before backing up the underlying storage.")
public class FastForwardTimestamp extends SingleBackendCommand {

    private static final Logger log = LoggerFactory.getLogger(FastForwardTimestamp.class);

    @Option(name = {"-t", "--timestamp"},
            title = "TIMESTAMP",
            description = "The timestamp to fast forward the persistent timestamp service to")
    Long timestamp;

    @Option(name = {"-f", "--file"},
            title = "TIMESTAMP_FILE",
            description = "A file containing the timestamp to fast forward the persistent timestamp service to")
    File file;

    @Override
    public int execute(AtlasDbServices services) {
        validateOptions();
        if (file != null) {
            setTimestampFromFile();
        }

        TimestampService ts = services.getTimestampService();
        if (!(ts instanceof PersistentTimestampService)) {
            log.error("Timestamp service must be of type {}, but yours is {}.  Exiting.",
                    PersistentTimestampService.class.toString(), ts.getClass().toString());
            return 1;
        }
        PersistentTimestampService pts = (PersistentTimestampService) ts;

        pts.fastForwardTimestamp(timestamp);
        log.info("Timestamp succesfully forwarded to {}", timestamp);

        return 0;
    }

    private void validateOptions() {
        if ((timestamp == null && file == null)
                || (timestamp != null && file != null)) {
            throw new IllegalArgumentException("You must specify one and only one of either a timestamp or a timestamp file.");
        }
    }

    private void setTimestampFromFile() {
        String timestampString;
        try {
            timestampString = StringUtils.strip(Files.readFirstLine(file, StandardCharsets.UTF_8));
        } catch (IOException e) {
            log.error("IOException thrown reading timestamp from file: {}", file.getPath());
            throw Throwables.propagate(e);
        }
        timestamp = Long.parseLong(timestampString);
     }

}