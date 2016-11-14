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
package com.palantir.atlasdb.cli.command.timestamp;

import com.palantir.timestamp.AdministrativeTimestampService;
import com.palantir.timestamp.TimestampService;

public final class TimestampUtils {
    private TimestampUtils() {
        // utility
    }

    public static void fastForwardTimestamp(TimestampService timestampService, long timestamp) {
        if (!(timestampService instanceof AdministrativeTimestampService)) {
            throw new IllegalStateException("Timestamp service does not have administrative capabilities!");
        }
        AdministrativeTimestampService administrativeTimestampService
                = (AdministrativeTimestampService) timestampService;
        administrativeTimestampService.fastForwardTimestamp(timestamp);
    }
}
