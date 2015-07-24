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
