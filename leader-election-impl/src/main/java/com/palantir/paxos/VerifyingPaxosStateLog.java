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
import java.sql.Connection;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.base.FunctionCheckedException;
import com.palantir.common.persist.Persistable;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;

/**
 * This implementation of {@link PaxosStateLog} uses one delegate as the source of truth, but also delegates all calls
 * to the experimental delegate and verifies consistency.
 *
 * NOTE: while the read write lock guarantees atomicity in the absence of failures, the experimental log could still
 * get out of sync with the source of truth if a write operation is performed on only one of the logs. Write operations
 * have therefore been implemented to allow for simple re-hydration of the experimental log -- writes are performed on
 * the current log first, while truncates are performed on the experimental log first.
 */
public final class VerifyingPaxosStateLog<V extends Persistable & Versionable> implements PaxosStateLog<V> {
    private static final Logger log = LoggerFactory.getLogger(VerifyingPaxosStateLog.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock(false);
    private final PaxosStateLog<V> currentLog;
    private final PaxosStateLog<V> experimentalLog;
    private final Persistable.Hydrator<V> hydrator;

    public VerifyingPaxosStateLog(Settings<V> settings) {
        this.currentLog = settings.currentLog();
        this.experimentalLog = settings.experimentalLog();
        this.hydrator = settings.hydrator();
    }

    public static <V extends Persistable & Versionable> PaxosStateLog<V> createWithMigration(
            PaxosStorageParameters parameters,
            Persistable.Hydrator<V> hydrator) {
        String logDirectory = parameters.fileBasedLogDirectory()
                .orElseThrow(() -> new SafeIllegalStateException("We currently need to have file-based storage"));
        Supplier<Connection> conn = SqliteConnections
                .createDefaultNamedSqliteDatabaseAtPath(parameters.sqliteBasedLogDirectory());
        NamespaceAndUseCase namespaceUseCase = parameters.namespaceAndUseCase();

        Settings<V> settings = ImmutableSettings.<V>builder()
                .currentLog(new PaxosStateLogImpl<>(logDirectory))
                .experimentalLog(SqlitePaxosStateLog.create(namespaceUseCase, conn))
                .hydrator(hydrator)
                .build();

        PaxosStateLogMigrator.MigrationContext<V> migrationContext = ImmutableMigrationContext.<V>builder()
                .sourceLog(settings.currentLog())
                .destinationLog(settings.experimentalLog())
                .hydrator(settings.hydrator())
                .migrationState(SqlitePaxosStateLogMigrationState.create(namespaceUseCase, conn))
                .build();
        PaxosStateLogMigrator.migrateToValidation(migrationContext);

        return new VerifyingPaxosStateLog<>(settings);
    }

    @Override
    public void writeRound(long seq, V round) {
        lock.writeLock().lock();
        try {
            currentLog.writeRound(seq, round);
            try {
                experimentalLog.writeRound(seq, round);
            } catch (RuntimeException e) {
                log.warn("Succeeded writing round for sequence number {} to the current log, but failed to write to "
                        + "experimental. This may cause future verification for this round to fail.",
                        SafeArg.of("sequence", seq), e);
                // suppress as failure in experimental service should not degrade the entire service
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public byte[] readRound(long seq) throws IOException {
        lock.readLock().lock();
        try {
            byte[] result = currentLog.readRound(seq);
            byte[] experimentalResult = safeReadFromExperimental(psl -> psl.readRound(seq));
            if (!Arrays.equals(result, experimentalResult)) {
                log.error("Mismatch in reading round for sequence number {} between legacy and current "
                        + "implementations. Legacy result {}, current result {}.",
                        SafeArg.of("sequence", seq),
                        UnsafeArg.of("legacy", hydrateIfNotNull(result)),
                        UnsafeArg.of("current", hydrateIfNotNull(experimentalResult)));
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long getLeastLogEntry() {
        return getExtremeLogEntry(PaxosStateLog::getLeastLogEntry);
    }

    @Override
    public long getGreatestLogEntry() {
        return getExtremeLogEntry(PaxosStateLog::getGreatestLogEntry);
    }

    @Override
    public void truncate(long toDeleteInclusive) {
        lock.writeLock().lock();
        try {
            // it is low risk to allow failure on experimental to propagate here
            experimentalLog.truncate(toDeleteInclusive);
            currentLog.truncate(toDeleteInclusive);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private <T> T safeReadFromExperimental(FunctionCheckedException<PaxosStateLog<V>, T, IOException> operation) {
        try {
            return operation.apply(experimentalLog);
        } catch (RuntimeException | IOException e) {
            log.warn("Succeeded a read operation on the current log, but failed on experimental. This will likely "
                    + "cause the subsequent verification to fail.", e);
            // suppress as failure in experimental service should not degrade the entire service
        }
        return null;
    }

    private V hydrateIfNotNull(byte[] result) {
        if (result != null) {
            return hydrator.hydrateFromBytes(result);
        }
        return null;
    }

    private long getExtremeLogEntry(Function<PaxosStateLog<V>, Long> extractor) {
        lock.readLock().lock();
        try {
            Long result = extractor.apply(currentLog);
            Long experimentalResult = safeReadFromExperimental(extractor::apply);
            if (!Objects.equals(result, experimentalResult) && result != PaxosAcceptor.NO_LOG_ENTRY) {
                log.error("Mismatch in getting the extreme log entry between legacy and current implementations."
                                + " Legacy result {}, current result {}.",
                        SafeArg.of("legacy", result),
                        SafeArg.of("current", experimentalResult));
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Value.Immutable
    public interface Settings<V extends Persistable & Versionable> {
        PaxosStateLog<V> currentLog();
        PaxosStateLog<V> experimentalLog();
        Persistable.Hydrator<V> hydrator();
    }
}
