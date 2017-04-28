/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
package com.palantir.paxos;

import java.io.IOException;

import com.palantir.common.persist.Persistable;

public interface PaxosStateLog<V extends Persistable & Versionable> {

    public class CorruptLogFileException extends IOException {
        private static final long serialVersionUID = 1L;
    }

    /**
     * Persists the given round to disk
     *
     * @param seq the sequence number of the round in question
     * @param round the round in question
     */
    public void writeRound(long seq, V round);

    /**
     * Retrieves the round corresponding to the given sequence from disk
     *
     * @param seq the sequence number of the round in question
     * @return the bytes of data for the given round
     * @throws CorruptLogFileException if the round for the given sequence number is corrupted on
     *         disk
     */
    public byte[] readRound(long seq) throws IOException;

    /**
     * @return the sequence number of the least known log entry or {@value PaxosAcceptor#NO_LOG_ENTRY}
     * if this log has never been truncated.
     */
    public long getLeastLogEntry();

    /**
     * @return the sequence number of the greatest known log entry or {@value PaxosAcceptor#NO_LOG_ENTRY}
     * if no entry is known
     */
    public long getGreatestLogEntry();

    /**
     * Deletes all rounds with sequence number less than or equal to seq.
     * @param toDeleteInclusive the upper bound sequence number (inclusive)
     */
    public void truncate(long toDeleteInclusive);

}
