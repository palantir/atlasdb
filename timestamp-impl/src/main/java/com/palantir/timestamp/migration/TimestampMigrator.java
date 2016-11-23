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
package com.palantir.timestamp.migration;

import java.util.Objects;

import com.google.common.base.Preconditions;
import com.palantir.timestamp.TimestampAdminService;

public class TimestampMigrator {
    private final TimestampAdminService sourceService;
    private final TimestampAdminService destinationService;

    public TimestampMigrator(TimestampAdminService sourceService, TimestampAdminService destinationService) {
        Preconditions.checkArgument(!Objects.equals(sourceService, destinationService),
                "Cannot migrate a timestamp admin service to itself!");
        this.sourceService = sourceService;
        this.destinationService = destinationService;
    }

    public void migrateTimestamps() {
        long targetTimestamp = sourceService.getUpperBoundTimestamp();
        destinationService.fastForwardTimestamp(targetTimestamp);
        sourceService.invalidateTimestamps();
    }
}
