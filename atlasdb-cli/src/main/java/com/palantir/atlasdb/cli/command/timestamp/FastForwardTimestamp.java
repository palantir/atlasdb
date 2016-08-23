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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

<<<<<<< 7033b8fc57203bf309772ac48101c6126fb91d56
import com.palantir.atlasdb.services.AtlasDbServices;
=======
import com.palantir.atlasdb.cli.services.AtlasDbServices;
>>>>>>> merge develop into perf cli branch (#820)
import com.palantir.timestamp.PersistentTimestampService;
import com.palantir.timestamp.TimestampService;

import io.airlift.airline.Command;

@Command(name = "fast-forward", description = "Fast forward the stored upper limit of a persistent timestamp"
        + " service to the specified timestamp.  Is used in the restore process to ensure that all future timestamps used are"
        + " explicity greater than any that may have been used to write data to the KVS before backing up the underlying storage.")
public class FastForwardTimestamp extends AbstractTimestampCommand {
    private static final Logger log = LoggerFactory.getLogger(FastForwardTimestamp.class);

    @Override
    public boolean isOnlineRunSupported() {
        return false;
    }

    @Override
    protected boolean requireTimestamp() {
        return true;
    }

    @Override
    protected int executeTimestampCommand(AtlasDbServices services) {
        TimestampService ts = services.getTimestampService();
        if (!(ts instanceof PersistentTimestampService)) {
            log.error("Timestamp service must be of type {}, but yours is {}.  Exiting.",
                    PersistentTimestampService.class.toString(), ts.getClass().toString());
            return 1;
        }
        PersistentTimestampService pts = (PersistentTimestampService) ts;

        pts.fastForwardTimestamp(timestamp);
        log.info("Timestamp succesfully fast-forwarded to {}", timestamp);
        return 0;
    }
}
