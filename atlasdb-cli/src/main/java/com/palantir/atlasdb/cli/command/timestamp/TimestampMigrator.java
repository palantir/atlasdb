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

public class TimestampMigrator {
    private final TimestampServicesProvider source;
    private final TimestampServicesProvider destination;

    public TimestampMigrator(TimestampServicesProvider source, TimestampServicesProvider destination) {
        this.source = source;
        this.destination = destination;
    }

    public void migrateTimestamps() {
        long migrationTimestamp = source.timestampService().getFreshTimestamp();
        migrateSpecificTimestamp(migrationTimestamp);
    }

    private void migrateSpecificTimestamp(long targetTimestamp) {
        destination.timestampAdminService().fastForwardTimestamp(targetTimestamp);
        source.timestampAdminService().invalidateTimestamps();
    }

}
