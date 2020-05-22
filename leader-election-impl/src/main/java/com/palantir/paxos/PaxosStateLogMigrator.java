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
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.immutables.value.Value;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;

public final class PaxosStateLogMigrator<V extends Persistable & Versionable> {
    @VisibleForTesting
    static final int BATCH_SIZE = 10_000;

    private final PaxosStateLog<V> sourceLog;
    private final PaxosStateLog<V> destinationLog;

    private PaxosStateLogMigrator(PaxosStateLog<V> sourceLog, PaxosStateLog<V> destinationLog) {
        this.sourceLog = sourceLog;
        this.destinationLog = destinationLog;
    }

    public static <V extends Persistable & Versionable> long migrateAndReturnCutoff(MigrationContext<V> context) {
        PaxosStateLogMigrator<V> migrator = new PaxosStateLogMigrator<>(context.sourceLog(), context.destinationLog());
        if (!context.migrationState().isInMigratedState()) {
            long migrationLowerBound = context.migrateFrom().orElse(context.sourceLog().getGreatestLogEntry());
            migrator.runMigration(migrationLowerBound, context.hydrator());
            context.migrationState().setCutoff(migrationLowerBound);
            context.migrationState().migrateToMigratedState();
            return migrationLowerBound;
        }
        return context.migrationState().getCutoff();
    }

    private void runMigration(long lowerBound, Persistable.Hydrator<V> hydrator) {
        destinationLog.truncate(destinationLog.getGreatestLogEntry());
        long upperBound = sourceLog.getGreatestLogEntry();
        if (upperBound == PaxosAcceptor.NO_LOG_ENTRY) {
            return;
        }

        LogReader<V> reader = new LogReader<>(sourceLog, hydrator);
        List<PaxosRound<V>> roundsToMigrate = LongStream.rangeClosed(lowerBound, upperBound)
                .mapToObj(reader::read)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        Iterables.partition(roundsToMigrate, BATCH_SIZE).forEach(destinationLog::writeBatchOfRounds);
    }

    @Value.Immutable
    interface MigrationContext<V extends Persistable & Versionable> {
        PaxosStateLog<V> sourceLog();
        PaxosStateLog<V> destinationLog();
        Persistable.Hydrator<V> hydrator();
        SqlitePaxosStateLogMigrationState migrationState();
        OptionalLong migrateFrom();
    }

    private static final class LogReader<V extends Persistable & Versionable> {
        private final PaxosStateLog<V> delegate;
        private final Persistable.Hydrator<V> hydrator;

        private LogReader(PaxosStateLog<V> delegate, Persistable.Hydrator<V> hydrator) {
            this.delegate = delegate;
            this.hydrator = hydrator;
        }

        private Optional<PaxosRound<V>> read(long sequence) {
            try {
                return Optional.ofNullable(delegate.readRound(sequence))
                        .map(bytes -> PaxosRound.of(sequence, hydrator.hydrateFromBytes(bytes)));
            } catch (IOException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
        }
    }
}
