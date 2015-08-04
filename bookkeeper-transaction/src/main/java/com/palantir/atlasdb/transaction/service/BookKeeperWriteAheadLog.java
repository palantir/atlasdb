/**
 * // Copyright 2015 Palantir Technologies
 * //
 * // Licensed under the BSD-3 License (the "License");
 * // you may not use this file except in compliance with the License.
 * // You may obtain a copy of the License at
 * //
 * // http://opensource.org/licenses/BSD-3-Clause
 * //
 * // Unless required by applicable law or agreed to in writing, software
 * // distributed under the License is distributed on an "AS IS" BASIS,
 * // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * // See the License for the specific language governing permissions and
 * // limitations under the License.
 */
package com.palantir.atlasdb.transaction.service;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.common.base.Throwables;


public class BookKeeperWriteAheadLog implements WriteAheadLog {
    private LedgerHandle handle;
    private boolean closed;

    /* package */ BookKeeperWriteAheadLog(LedgerHandle handle, boolean closed) {
        this.handle = handle;
        this.closed = closed;
    }

    private static class EmptyIterator implements Iterator<TransactionLogEntry> {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public TransactionLogEntry next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public void append(long startTs, long commitTs) {
        // encode start and commit timestamp
        byte[] startTsBytes = PtBytes.toBytes(startTs);
        byte[] commitTsBytes = PtBytes.toBytes(commitTs);
        byte[] resultBytes = new byte[startTsBytes.length + commitTsBytes.length];
        System.arraycopy(startTsBytes, 0, resultBytes, 0, startTsBytes.length);
        System.arraycopy(commitTsBytes, 0, resultBytes, startTsBytes.length, commitTsBytes.length);

        try {
            handle.addEntry(resultBytes);
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        } catch(BKException.BKLedgerClosedException e) {
            throw new IllegalStateException("Cannot append to a closed log");
        } catch (BKException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public void close() {
        try {
            handle.close();
            closed = true;
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        } catch(BKException.BKLedgerClosedException e) {
            throw new IllegalStateException("The log is already closed");
        } catch (BKException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public long getId() {
        return handle.getId();
    }

    @Override
    public Iterator<TransactionLogEntry> iterator() {
        if (!closed)
            throw new IllegalStateException("Cannot visit log entries until log is closed");

        try {
            long lastConfirmed = handle.readLastConfirmed();
            if (lastConfirmed == LedgerHandle.INVALID_ENTRY_ID)
                return new EmptyIterator();

            final Enumeration<LedgerEntry> entries = handle.readEntries(0, lastConfirmed);
            return new Iterator<TransactionLogEntry>() {
                @Override
                public boolean hasNext() {
                    return entries.hasMoreElements();
                }

                @Override
                public TransactionLogEntry next() {
                    LedgerEntry entry = entries.nextElement();

                    // decode start and commit timestamp
                    byte[] entryBytes = entry.getEntry();
                    long startTs = PtBytes.toLong(entryBytes, 0);
                    long commitTs = PtBytes.toLong(entryBytes, PtBytes.SIZEOF_LONG);
                    return new TransactionLogEntry(startTs, commitTs);
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        } catch (InterruptedException e) {
            throw Throwables.throwUncheckedException(e);
        } catch (BKException e) {
            throw Throwables.throwUncheckedException(e);
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }


}
