/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
import java.io.IOException;

public interface PaxosStateLog<V extends Persistable & Versionable> {

    class CorruptLogFileException extends IOException {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Persists the given round to disk.
     *
     * @param seq the sequence number of the round in question
     * @param round the round in question
     */
    void writeRound(long seq, V round);

    /**
     * Implementations of this method MUST obey the following:
     * 1) The end state of the log should be equivalent to applying the operations in iteration order.
     * 2) Implementations need not be atomic. However, if they fail, they should apply only prefixes of the input
     * iterable.
     */
    default void writeBatchOfRounds(Iterable<PaxosRound<V>> rounds) {
        rounds.forEach(round -> writeRound(round.sequence(), round.value()));
    }

    /**
     * Retrieves the round corresponding to the given sequence from disk.
     *
     * @param seq the sequence number of the round in question
     * @return the bytes of data for the given round
     * @throws CorruptLogFileException if the round for the given sequence number is corrupted on
     *         disk
     */
    byte[] readRound(long seq) throws IOException;

    /**
     * Returns the sequence number of the least known log entry or {@value PaxosAcceptor#NO_LOG_ENTRY}
     * if this log has never been truncated.
     */
    long getLeastLogEntry();

    /**
     * Returns the sequence number of the greatest known log entry or {@value PaxosAcceptor#NO_LOG_ENTRY}
     * if no entry is known.
     */
    long getGreatestLogEntry();

    /**
     * Deletes all rounds with sequence number less than or equal to seq.
     *
     * @param toDeleteInclusive the upper bound sequence number (inclusive)
     */
    void truncate(long toDeleteInclusive);

    void truncateAllRounds();

    /**
     * Returns true iff this Paxos state log is in a state where it is able to service requests. Behaviour of other
     * methods is undefined if the log is not initialized yet.
     */
    default boolean isInitialized() {
        return true;
    }
}
