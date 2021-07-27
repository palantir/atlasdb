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

import com.palantir.common.persist.Persistable;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.logger.SafeLogger;
import com.palantir.logsafe.logger.SafeLoggerFactory;
import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import org.immutables.value.Value;

/**
 * This implementation of {@link PaxosStateLog} delegates all reads and writes of rounds to one of two delegates, as
 * determined by the cutoff point. If a read or write does occur prior to the cutoff point, i.e., to the legacy delegate
 * we update the appropriate metric. Remaining methods are delegated only to the current delegate.
 */
public final class SplittingPaxosStateLog<V extends Persistable & Versionable> implements PaxosStateLog<V> {
    private static final SafeLogger log = SafeLoggerFactory.get(SplittingPaxosStateLog.class);

    private final PaxosStateLog<V> legacyLog;
    private final PaxosStateLog<V> currentLog;
    private final Runnable markLegacyWrite;
    private final Runnable markLegacyRead;
    private final long cutoffInclusive;
    private final AtomicLong legacyLogLeastLogEntry;

    private SplittingPaxosStateLog(
            PaxosStateLog<V> legacyLog,
            PaxosStateLog<V> currentLog,
            Runnable markLegacyWrite,
            Runnable markLegacyRead,
            long cutoffInclusive,
            AtomicLong legacyLogLeastLogEntry) {
        this.legacyLog = legacyLog;
        this.currentLog = currentLog;
        this.markLegacyWrite = markLegacyWrite;
        this.markLegacyRead = markLegacyRead;
        this.cutoffInclusive = cutoffInclusive;
        this.legacyLogLeastLogEntry = legacyLogLeastLogEntry;
    }

    public static <V extends Persistable & Versionable> PaxosStateLog<V> create(SplittingParameters<V> parameters) {
        Preconditions.checkState(
                parameters.cutoffInclusive() == PaxosAcceptor.NO_LOG_ENTRY
                        || parameters.currentLog().getGreatestLogEntry() >= parameters.cutoffInclusive(),
                "Cutoff value must either be -1, or the current log must contain an entry after the cutoff.");
        return new SplittingPaxosStateLog<>(
                parameters.legacyLog(),
                parameters.currentLog(),
                parameters.legacyOperationMarkers().markLegacyWrite(),
                parameters.legacyOperationMarkers().markLegacyRead(),
                parameters.cutoffInclusive(),
                new AtomicLong(parameters.legacyLog().getLeastLogEntry()));
    }

    public static <V extends Persistable & Versionable> PaxosStateLog<V> createWithMigration(
            PaxosStorageParameters params,
            Persistable.Hydrator<V> hydrator,
            LegacyOperationMarkers legacyOperationMarkers,
            OptionalLong migrateFrom) {
        String logDirectory = params.fileBasedLogDirectory()
                .orElseThrow(() -> new SafeIllegalStateException("We currently need to have file-based storage"));
        NamespaceAndUseCase namespaceUseCase = params.namespaceAndUseCase();

        PaxosStateLogMigrator.MigrationContext<V> migrationContext = ImmutableMigrationContext.<V>builder()
                .sourceLog(PaxosStateLogImpl.createFileBacked(logDirectory))
                .destinationLog(SqlitePaxosStateLog.create(namespaceUseCase, params.sqliteDataSource()))
                .hydrator(hydrator)
                .migrationState(SqlitePaxosStateLogMigrationState.create(namespaceUseCase, params.sqliteDataSource()))
                .migrateFrom(migrateFrom)
                .namespaceAndUseCase(namespaceUseCase)
                .skipValidationAndTruncateSourceIfMigrated(params.skipConsistencyCheckAndTruncateOldPaxosLog())
                .build();

        long cutoff = PaxosStateLogMigrator.migrateAndReturnCutoff(migrationContext);

        if (params.skipConsistencyCheckAndTruncateOldPaxosLog()) {
            return migrationContext.destinationLog();
        }

        SplittingParameters<V> splittingParameters = ImmutableSplittingParameters.<V>builder()
                .legacyLog(migrationContext.sourceLog())
                .currentLog(migrationContext.destinationLog())
                .cutoffInclusive(cutoff)
                .legacyOperationMarkers(legacyOperationMarkers)
                .build();

        return SplittingPaxosStateLog.create(splittingParameters);
    }

    @Override
    public void writeRound(long seq, V round) {
        if (seq >= cutoffInclusive) {
            currentLog.writeRound(seq, round);
        } else {
            markLegacyWrite.run();
            legacyLog.writeRound(seq, round);
            legacyLogLeastLogEntry.accumulateAndGet(seq, Math::min);
        }
    }

    @Override
    public byte[] readRound(long seq) throws IOException {
        if (seq >= cutoffInclusive) {
            return currentLog.readRound(seq);
        } else {
            markLegacyRead.run();
            return legacyLog.readRound(seq);
        }
    }

    @Override
    public long getLeastLogEntry() {
        return Math.min(legacyLogLeastLogEntry.get(), cutoffInclusive);
    }

    @Override
    public long getGreatestLogEntry() {
        return currentLog.getGreatestLogEntry();
    }

    /**
     * This implementation is a noop to ensure correctness of {@link #getLeastLogEntry()}.
     */
    @Override
    public void truncate(long toDeleteInclusive) {
        log.warn("Tried to truncate paxos state log with an implementation that does not support truncations.");
    }

    @Override
    public void truncateAllRounds() {
        log.warn("Tried to truncate paxos state log with an implementation that does not support truncations.");
    }

    @Value.Immutable
    interface SplittingParameters<V extends Persistable & Versionable> {
        PaxosStateLog<V> legacyLog();

        PaxosStateLog<V> currentLog();

        LegacyOperationMarkers legacyOperationMarkers();

        long cutoffInclusive();
    }

    @Value.Immutable
    public interface LegacyOperationMarkers {
        Runnable markLegacyWrite();

        Runnable markLegacyRead();
    }
}
