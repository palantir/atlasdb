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
package com.palantir.atlasdb.migration;

import com.palantir.timestamp.TimestampAdminService;

public class TimelockMigrationTask {
    private final TimestampAdminService sourceService;
    private final TimestampAdminService destinationService;

    public TimelockMigrationTask(TimestampAdminService sourceService, TimestampAdminService destinationService) {
        this.sourceService = sourceService;
        this.destinationService = destinationService;
    }

    public void runMigrationTask() {
        long timestamp = sourceService.upperBoundTimestamp();
        destinationService.fastForwardTimestamp(timestamp);
        sourceService.invalidateTimestamps();
    }
}
