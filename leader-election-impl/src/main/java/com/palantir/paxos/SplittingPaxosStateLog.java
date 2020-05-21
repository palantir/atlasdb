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

import com.palantir.common.persist.Persistable;

/**
 * This implementation of {@link PaxosStateLog} delegates all reads and writes of rounds to one of two delegates, as
 * determined by the cutoff point. If a read or write does occur prior to the cutoff point, i.e., to the legacy delegate
 * e update the appropriate metric. Remaining methods are delegated only to the current delegate.
 */
public class SplittingPaxosStateLog<V extends Persistable & Versionable> implements PaxosStateLog<V> {
    private final PaxosStateLog<V> legacyLog;
    private final PaxosStateLog<V> currentLog;
    private final Runnable markLegacyWrite;
    private final Runnable markLegacyRead;
    private final long cutoffInclusive;

    public SplittingPaxosStateLog(PaxosStateLog<V> legacyLog,
            PaxosStateLog<V> currentLog,
            Runnable markLegacyWrite,
            Runnable markLegacyRead,
            long cutoffInclusive) {
        this.legacyLog = legacyLog;
        this.currentLog = currentLog;
        this.markLegacyWrite = markLegacyWrite;
        this.markLegacyRead = markLegacyRead;
        this.cutoffInclusive = cutoffInclusive;
    }

    @Override
    public void writeRound(long seq, V round) {
        if (seq >= cutoffInclusive) {
            currentLog.writeRound(seq, round);
        } else {
            markLegacyWrite.run();
            legacyLog.writeRound(seq, round);
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
        return currentLog.getLeastLogEntry();
    }

    @Override
    public long getGreatestLogEntry() {
        return currentLog.getGreatestLogEntry();
    }

    @Override
    public void truncate(long toDeleteInclusive) {
        currentLog.truncate(toDeleteInclusive);
    }
}
