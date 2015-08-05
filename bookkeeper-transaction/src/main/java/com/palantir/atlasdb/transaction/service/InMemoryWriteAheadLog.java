/**
 * Copyright 2015 Palantir Technologies
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
package com.palantir.atlasdb.transaction.service;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import com.google.common.collect.Maps;


public class InMemoryWriteAheadLog implements WriteAheadLog {

    public static class InMemoryWriteAheadLogManager implements WriteAheadLogManager {
        private final Map<Long, InMemoryWriteAheadLog> logMap = Maps.newHashMap();
        private long nextLogId = 0;

        @Override
        public synchronized WriteAheadLog create() {
            InMemoryWriteAheadLog log = new InMemoryWriteAheadLog(nextLogId);
            logMap.put(nextLogId, log);
            nextLogId += 1;
            return log;
        }

        @Override
        public synchronized WriteAheadLog retrieve(long logId) {
            WriteAheadLog log = logMap.get(logId);
            if (log != null)
                log.close();
            return log;
        }
    }

    private final Queue<TransactionLogEntry> logEntries;
    private final long logId;
    private boolean closed;

    private InMemoryWriteAheadLog(long logId) {
        this.logId = logId;
        this.logEntries = new LinkedList<TransactionLogEntry>();
        this.closed = false;
    }

    @Override
    public synchronized void append(long startTs, long commitTs) {
        if (!closed) {
            logEntries.add(new TransactionLogEntry(startTs, commitTs));
       } else {
           throw new IllegalStateException("Cannot append to a closed log");
       }
    }

    @Override
    public synchronized void close() {
        closed = true;
    }

    @Override
    public synchronized Iterator<TransactionLogEntry> iterator() {
        if (!closed) {
            throw new IllegalStateException("Cannot visit log entries until log is closed");
        }
       return logEntries.iterator();
    }

    @Override
    public long getId() {
        return logId;
    }

    @Override
    public synchronized boolean isClosed() {
        return closed;
    }
}
