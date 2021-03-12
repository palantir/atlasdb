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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;
import com.palantir.logsafe.Preconditions;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.immutables.value.Value;
import org.immutables.value.Value.Default;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class PaxosStateLogMigrator<V extends Persistable & Versionable> {
    private static final Logger log = LoggerFactory.getLogger(PaxosStateLogMigrator.class);

    public static final int SAFETY_BUFFER = 50;

    @VisibleForTesting
    static final int BATCH_SIZE = 10_000;

    private final PaxosStateLog<V> sourceLog;
    private final PaxosStateLog<V> destinationLog;

    private PaxosStateLogMigrator(PaxosStateLog<V> sourceLog, PaxosStateLog<V> destinationLog) {
        this.sourceLog = sourceLog;
        this.destinationLog = destinationLog;
    }

    /**
     * Migrates entries from sourceLog to destinationLog if the migration has not been run already. Returns the cutoff
     * value, i.e., the lower bound of the migration. The cutoff value is guaranteed to be less than or equal to the
     * value of {@link MigrationContext#migrateFrom()}, and the migration is guaranteed to copy at least one entry if
     * sourceLog is not empty. If sourceLog is empty, cutoff will be {@link PaxosAcceptor#NO_LOG_ENTRY}.
     */
    public static <V extends Persistable & Versionable> long migrateAndReturnCutoff(MigrationContext<V> context) {
        PaxosStateLogMigrator<V> migrator = new PaxosStateLogMigrator<>(context.sourceLog(), context.destinationLog());
        if (!context.migrationState().isInMigratedState()) {
            log.info(
                    "Now migrating namespace and use case {} to sqlite.",
                    SafeArg.of("namespaceAndUseCase", context.namespaceAndUseCase()));
            long cutoff = calculateCutoff(context);
            migrator.runMigration(cutoff, context);
            context.migrationState().setCutoff(cutoff);
            context.migrationState().migrateToMigratedState();
            if (context.skipValidationAndTruncateSourceIfMigrated()) {
                context.sourceLog().truncateAllRounds();
            }
            log.info(
                    "Migrated namespace and use case {} to sqlite.",
                    SafeArg.of("namespaceAndUseCase", context.namespaceAndUseCase()));
            return cutoff;
        } else {
            if (!context.skipValidationAndTruncateSourceIfMigrated()) {
                log.info(
                        "Now validating the sqlite migration state for namespace and use case {}.",
                        SafeArg.of("namespaceAndUseCase", context.namespaceAndUseCase()));
                validateConsistency(context);
                log.info(
                        "Validated the sqlite migration state for namespace and use case {}.",
                        SafeArg.of("namespaceAndUseCase", context.namespaceAndUseCase()));
            } else {
                log.info(
                        "Now truncating the sqlite migration state for namespace and use case {}.",
                        SafeArg.of("namespaceAndUseCase", context.namespaceAndUseCase()));
                context.sourceLog().truncateAllRounds();
                log.info(
                        "Truncated the source log for namespace and use case {}.",
                        SafeArg.of("namespaceAndUseCase", context.namespaceAndUseCase()));
            }
        }
        return context.migrationState().getCutoff();
    }

    private static <V extends Persistable & Versionable> long calculateCutoff(MigrationContext<V> context) {
        long greatestEntryToMigrate = context.sourceLog().getGreatestLogEntry();
        long lowerBoundCandidate = context.migrateFrom().orElse(greatestEntryToMigrate) - SAFETY_BUFFER;
        long lowerBoundWithAtLeastOneEntry = Math.min(greatestEntryToMigrate, lowerBoundCandidate);
        return Math.max(PaxosAcceptor.NO_LOG_ENTRY, lowerBoundWithAtLeastOneEntry);
    }

    private void runMigration(long cutoff, MigrationContext<V> context) {
        destinationLog.truncate(destinationLog.getGreatestLogEntry());
        long lowerBound = cutoff == PaxosAcceptor.NO_LOG_ENTRY ? 0 : cutoff;
        long upperBound = sourceLog.getGreatestLogEntry();
        if (upperBound == PaxosAcceptor.NO_LOG_ENTRY) {
            return;
        }

        LogReader<V> reader = new LogReader<>(sourceLog, context.hydrator());
        log.info(
                "Reading entries for paxos state log migration for {}.",
                SafeArg.of("namespaceAndUseCase", context.namespaceAndUseCase()));
        Instant start = Instant.now();
        List<PaxosRound<V>> roundsToMigrate = LongStream.rangeClosed(lowerBound, upperBound)
                .mapToObj(reader::read)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        Instant afterRead = Instant.now();
        log.info(
                "Reading {} entries from file backed paxos state log took {} for {}.",
                SafeArg.of("numEntries", roundsToMigrate.size()),
                SafeArg.of("duration", Duration.between(start, afterRead)),
                SafeArg.of("namespaceAndUseCase", context.namespaceAndUseCase()));
        Lists.partition(roundsToMigrate, BATCH_SIZE)
                .forEach(batch -> writeBatchRetryingUpToFiveTimes(destinationLog, batch));
        log.info(
                "Writing {} entries to sqlite backed paxos state log took {} for {}.",
                SafeArg.of("numEntries", roundsToMigrate.size()),
                SafeArg.of("duration", Duration.between(afterRead, Instant.now())),
                SafeArg.of("namespaceAndUseCase", context.namespaceAndUseCase()));
    }

    private void writeBatchRetryingUpToFiveTimes(PaxosStateLog<V> target, List<PaxosRound<V>> batch) {
        for (int retryCount = 0; retryCount < 5; retryCount++) {
            try {
                target.writeBatchOfRounds(batch);
                return;
            } catch (Exception e) {
                log.info("Failed to write a migration batch. Retrying after backoff.", e);
                Uninterruptibles.sleepUninterruptibly(Duration.ofMillis(100));
            }
        }
        target.writeBatchOfRounds(batch);
    }

    private static <V extends Persistable & Versionable> void validateConsistency(MigrationContext<V> context) {
        long migrationCutoff = calculateCutoff(context);
        long persistedCutoff = context.migrationState().getCutoff();
        long greatestSourceEntry = context.sourceLog().getGreatestLogEntry();
        Preconditions.checkState(
                migrationCutoff <= persistedCutoff || context.migrateFrom().isPresent(),
                "The migration to the destination state log was already performed in the past, but the "
                        + "persisted cutoff value does not match a newly calculated one. This indicates the source "
                        + "log has advanced since the migration was performed which could lead to data corruption if "
                        + "allowed to continue.",
                SafeArg.of("fresh cutoff", migrationCutoff),
                SafeArg.of("persisted cutoff", persistedCutoff),
                SafeArg.of("source greatest entry", greatestSourceEntry),
                SafeArg.of("namespaceAndUseCase", context.namespaceAndUseCase()));
        if (greatestSourceEntry == PaxosAcceptor.NO_LOG_ENTRY) {
            return;
        }
        try {
            V source = context.hydrator().hydrateFromBytes(context.sourceLog().readRound(greatestSourceEntry));
            byte[] destinationBytes = context.destinationLog().readRound(greatestSourceEntry);
            V dest = destinationBytes != null ? context.hydrator().hydrateFromBytes(destinationBytes) : null;
            Preconditions.checkState(
                    source.equalsIgnoringVersion(dest),
                    "The migration to the destination state log was already performed in the past, but the "
                            + "entry with the greatest sequence in source log does not match the entry in the "
                            + "destination log. This indicates the source log has advanced since the migration was "
                            + "performed which could lead to data corruption if allowed to continue.",
                    SafeArg.of("source entry", source),
                    SafeArg.of("destination entry", dest),
                    SafeArg.of("namespaceAndUseCase", context.namespaceAndUseCase()));
        } catch (IOException e) {
            throw new SafeIllegalArgumentException("Unable to verify consistency between source and destination paxos "
                    + "logs because the source log entry could not be read.");
        }
    }

    @Value.Immutable
    interface MigrationContext<V extends Persistable & Versionable> {
        PaxosStateLog<V> sourceLog();

        PaxosStateLog<V> destinationLog();

        Persistable.Hydrator<V> hydrator();

        SqlitePaxosStateLogMigrationState migrationState();

        OptionalLong migrateFrom();

        NamespaceAndUseCase namespaceAndUseCase();

        @Default
        default boolean skipValidationAndTruncateSourceIfMigrated() {
            return false;
        }
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
