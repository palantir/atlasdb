/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.paxos;

import java.util.stream.LongStream;

import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.common.persist.Persistable;

public final class PaxosStateLogMigrator<V extends Persistable & Versionable> {
    @VisibleForTesting
    static final int BATCH_SIZE = 10_000;

    private final PaxosStateLog<V> sourceLog;
    private final PaxosStateLog<V> destinationLog;
    private final Persistable.Hydrator<V> hydrator;
    private final SqlitePaxosStateLogMigrationState migrationState;

    private PaxosStateLogMigrator(PaxosStateLog<V> sourceLog,
            PaxosStateLog<V> destinationLog,
            Persistable.Hydrator<V> hydrator,
            SqlitePaxosStateLogMigrationState migrationState) {
        this.sourceLog = sourceLog;
        this.destinationLog = destinationLog;
        this.hydrator = hydrator;
        this.migrationState = migrationState;
    }

    public static <V extends Persistable & Versionable> void migrate(MigrationContext<V> migrationContext) {
        PaxosStateLogMigrator<V> migrator = new PaxosStateLogMigrator<>(
                migrationContext.sourceLog(),
                migrationContext.destinationLog(),
                migrationContext.hydrator(),
                migrationContext.migrationState());
        migrator.runMigration();
    }

    private void runMigration() {
        if (migrationState.hasMigratedFromInitialState()) {
            return;
        }

        destinationLog.truncate(destinationLog.getGreatestLogEntry());
        long lowerBound = lowestSequenceToMigrate();
        long upperBound = sourceLog.getGreatestLogEntry();
        if (upperBound == PaxosAcceptor.NO_LOG_ENTRY) {
            migrationState.migrateToValidationState();
            return;
        }

        try (PaxosStateLogBatchReader<V> reader = new PaxosStateLogBatchReader<>(sourceLog, hydrator, 100)) {
            long numberOfBatches = (upperBound - lowerBound) / BATCH_SIZE + 1;
            LongStream.iterate(lowerBound, x -> x + BATCH_SIZE)
                    .limit(numberOfBatches)
                    .mapToObj(sequence -> reader.readBatch(sequence, BATCH_SIZE))
                    .forEach(destinationLog::writeBatchOfRounds);
        }
        migrationState.migrateToValidationState();
    }

    private long lowestSequenceToMigrate() {
        long leastLogEntry = sourceLog.getLeastLogEntry();
        return leastLogEntry == PaxosAcceptor.NO_LOG_ENTRY ? 0L : leastLogEntry;
    }

    @Value.Immutable
    interface MigrationContext<V extends Persistable & Versionable> {
        PaxosStateLog<V> sourceLog();
        PaxosStateLog<V> destinationLog();
        Persistable.Hydrator<V> hydrator();
        SqlitePaxosStateLogMigrationState migrationState();
    }
}
