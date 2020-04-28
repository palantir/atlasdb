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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.immutables.value.Value;

import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;

public class ToSqlitePaxosStateLogMigrator<V extends Persistable & Versionable> {
    private static final int BATCH_SIZE = 10_000;

    private final PaxosStateLog<V> sourceLog;
    private final SqlitePaxosStateLog<V> destinationLog;
    private final Persistable.Hydrator<V> hydrator;
    private final SqlitePaxosStateLogMigrationState migrationState;

    public ToSqlitePaxosStateLogMigrator(PaxosStateLog<V> sourceLog,
            SqlitePaxosStateLog<V> destinationLog,
            Persistable.Hydrator<V> hydrator,
            SqlitePaxosStateLogMigrationState migrationState) {
        this.sourceLog = sourceLog;
        this.destinationLog = destinationLog;
        this.hydrator = hydrator;
        this.migrationState = migrationState;
    }

    public static <V extends Persistable & Versionable> void migrateToSqlite(MigrationContext<V> migrationContext) {
        ToSqlitePaxosStateLogMigrator<V> migrator = new ToSqlitePaxosStateLogMigrator<>(
                migrationContext.sourceLog(),
                migrationContext.destinationLog(),
                migrationContext.hydrator(),
                migrationContext.migrationState());
        try {
            migrator.runMigration();
        } catch (IOException e) {
            Throwables.rewrapAndThrowUncheckedException(e);
        }
    }

    private void runMigration() throws IOException {
        if (migrationState.hasAlreadyMigrated()) {
            return;
        }
        destinationLog.reinitialize();
        long lowerBound = sourceLog.getLeastLogEntry();
        long upperBound = sourceLog.getGreatestLogEntry();
        List<PaxosRound<V>> buffer = new ArrayList<>();
        for (long currentSequence = lowerBound; currentSequence <= upperBound; currentSequence++) {
            byte[] valueForRound = sourceLog.readRound(currentSequence);
            if (valueForRound == null) {
                continue;
            }
            buffer.add(ImmutablePaxosRound.<V>builder()
                    .sequence(currentSequence)
                    .value(hydrator.hydrateFromBytes(valueForRound))
                    .build());
            if (buffer.size() >= BATCH_SIZE) {
                destinationLog.writeBatchOfRounds(buffer);
                buffer.clear();
            }
        }
        destinationLog.writeBatchOfRounds(buffer);
    }

    @Value.Immutable
    interface MigrationContext<V extends Persistable & Versionable> {
        PaxosStateLog<V> sourceLog();
        SqlitePaxosStateLog<V> destinationLog();
        Persistable.Hydrator<V> hydrator();
        SqlitePaxosStateLogMigrationState migrationState();
    }
}
