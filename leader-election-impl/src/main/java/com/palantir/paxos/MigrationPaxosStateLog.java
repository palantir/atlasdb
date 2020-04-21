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
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.palantir.common.base.Throwables;
import com.palantir.common.persist.Persistable;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.UnsafeArg;

public class MigrationPaxosStateLog<V extends Persistable & Versionable> implements PaxosStateLog<V> {
    private final static Logger log = LoggerFactory.getLogger(MigrationPaxosStateLog.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock(false);
    private final PaxosStateLog<V> legacyLog;
    private final PaxosStateLog<V> currentLog;

    private MigrationPaxosStateLog(PaxosStateLog<V> legacyLog, PaxosStateLog<V> currentLog) {
        this.legacyLog = legacyLog;
        this.currentLog = currentLog;
    }

    public static <V extends Persistable & Versionable> PaxosStateLog<V> create(PaxosStateLog<V> legacyLog,
            PaxosStateLog<V> currentLog, Persistable.Hydrator<V> hydrator) {
        runMigration(legacyLog, currentLog, hydrator);
        return new MigrationPaxosStateLog<>(currentLog, legacyLog);
    }

    private static <V extends Persistable & Versionable> void runMigration(PaxosStateLog<V> legacyLog,
            PaxosStateLog<V> currentLog, Persistable.Hydrator<V> hydrator) {
        long leastSequence = legacyLog.getLeastLogEntry();
        long greatestSequence = legacyLog.getGreatestLogEntry();
        for (long sequence = leastSequence; sequence <= greatestSequence; sequence++) {
            try {
                byte[] bytes = legacyLog.readRound(sequence);
                if (bytes != null) {
                    currentLog.writeRound(sequence, hydrator.hydrateFromBytes(bytes));
                }
            } catch (IOException e) {
                log.error("Encountered exception while trying to read round {} from legacy log.",
                        SafeArg.of("sequence", sequence));
                // todo(gmaretic): decide if we want to throw or not
                Throwables.rewrapAndThrowUncheckedException(e);
            }
        }
    }

    @Override
    public void writeRound(long seq, V round) {
        lock.writeLock().lock();
        try {
            legacyLog.writeRound(seq, round);
            currentLog.writeRound(seq, round);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public byte[] readRound(long seq) throws IOException {
        lock.readLock().lock();
        try {
            byte[] legacyResult = legacyLog.readRound(seq);
            byte[] currentResult = currentLog.readRound(seq);
            if (!Arrays.equals(legacyResult, currentResult)) {
                log.error("Mismatch in reading round for sequence number {} between legacy and current "
                        + "implementations. Legacy result {}, current result {}.",
                        SafeArg.of("sequence", seq),
                        UnsafeArg.of("legacy", hydratePaxosValue(legacyResult)),
                        UnsafeArg.of("current", hydratePaxosValue(currentResult)));
            }
            return legacyResult;
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
            // We truncate currentLog first because legacyLog is the source of truth, so if this operation fails half
            // way, currentLog will be rehydrated and we will effectively undo the partial change.
            currentLog.truncate(toDeleteInclusive);
            legacyLog.truncate(toDeleteInclusive);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static Optional<PaxosValue> hydratePaxosValue(byte[] bytes) {
        return Optional.ofNullable(bytes).map(PaxosValue.BYTES_HYDRATOR::hydrateFromBytes);
    }

    private long getExtremeLogEntry(Function<PaxosStateLog<V>, Long> extractor) {
        lock.readLock().lock();
        try {
            long legacyResult = extractor.apply(legacyLog);
            long currentResult = extractor.apply(currentLog);
            if (legacyResult != currentResult) {
                log.error("Mismatch in getting the extreme log entry between legacy and current implementations."
                                + " Legacy result {}, current result {}.",
                        SafeArg.of("legacy", legacyResult),
                        SafeArg.of("current", currentResult));
            }
            return legacyResult;
        } finally {
            lock.readLock().unlock();
        }
    }
}
