/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.calcite;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.calcite.linq4j.Enumerator;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.palantir.atlasdb.api.AtlasDbService;
import com.palantir.atlasdb.api.RangeToken;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.api.TransactionToken;
import com.palantir.atlasdb.encoding.PtBytes;

public class PagingAtlasEnumerator implements Enumerator<AtlasRow> {
    private static final int BATCH_SIZE = 500;
    private static final int REQUEST_TIMEOUT_IN_SEC = 45;
    private static final int REQUEST_POLLING_INTERVAL_IN_SEC = 1;

    private enum State {
        /**
         *  The state when the enumerator has either just been initialized or has a current result.
         */
        ACTIVE,

        /**
         *  The state when the enumerator is out of results (or has been canceled), but not explicitly closed.
         */
        DONE,

        /**
         *  The state when the enumerator has been explicitly closed.
         *  A closed enumerator cannot be reset() as it has release its underlying resources.
         */
        CLOSED;
    }

    private final AtlasDbService service;
    private final AtlasTableMetadata metadata;
    private final TransactionToken transactionToken;
    private final TableRange initRangeRequest;
    private final AtomicBoolean cancelFlag;
    private final ExecutorService executor;
    private RangeToken rangeToken;
    private LocalAtlasEnumerator curIter;
    private State state;

    private PagingAtlasEnumerator(AtlasDbService service,
                                 AtlasTableMetadata metadata,
                                 TransactionToken transactionToken,
                                 AtomicBoolean cancelFlag,
                                 TableRange initRangeRequest,
                                 ExecutorService executor) {
        this.service = service;
        this.metadata = metadata;
        this.transactionToken = transactionToken;
        this.cancelFlag = cancelFlag;
        this.initRangeRequest = initRangeRequest;
        this.executor = executor;
        reset();
    }

    public static PagingAtlasEnumerator create(AtlasDbService service,
                                               AtlasTableMetadata metadata,
                                               String tableName,
                                               AtomicBoolean cancelFlag) {
        TransactionToken transactionToken = TransactionToken.autoCommit();
        TableRange initRequest = new TableRange(
                tableName,
                PtBytes.EMPTY_BYTE_ARRAY,
                PtBytes.EMPTY_BYTE_ARRAY,
                metadata.namedColumns().stream()
                        .map(AtlasColumnMetdata::getName)
                        .map(String::getBytes)
                        .collect(Collectors.toList()),
                BATCH_SIZE);
        return new PagingAtlasEnumerator(
                service,
                metadata,
                transactionToken,
                cancelFlag,
                initRequest,
                Executors.newCachedThreadPool());
    }

    @Override
    public AtlasRow current() {
        if (isClosed() || isDone() || curIter == null) {
            throw new NoSuchElementException();
        }
        return curIter.current();
    }

    @Override
    public boolean moveNext() {
        if (!moveNextInternal()) {
            rangeToken = null;
            curIter = null;
            switch (state) {
                case ACTIVE:
                    state = State.DONE;
                case DONE:
                case CLOSED:
                default:
                    return false;
            }
        }
        return true;
    }

    @Override
    public void reset() {
        if (state == State.CLOSED) {
            throw new UnsupportedOperationException(
                    "Cannot reset a closed iterator as the resources were already released.");
        }
        rangeToken = null;
        curIter = null;
        state = State.ACTIVE;
    }

    @Override
    public void close() {
        rangeToken = null;
        if (curIter != null) {
            curIter.close();
            curIter = null;
        }
        executor.shutdownNow();
        state = State.CLOSED;
    }

    private boolean moveNextInternal() {
        if (isClosed() || isDone()) {
            return false;
        }

        if (cancelFlag.get()) {
            return false;
        }

        if (rangeToken == null) {
            rangeToken = fetchNextRange(initRangeRequest);
            if (rangeToken == null) {
                return false;
            }
        }

        if (curIter == null) {
            curIter = nextIterator();
        }

        if (curIter.hasNext()) {
            curIter.moveNext();
            return true;
        } else { // page to the next range
            if (rangeToken.hasMoreResults()) {
                rangeToken = fetchNextRange(rangeToken.getNextRange());
                if (rangeToken == null) {
                    return false;
                }
                curIter = nextIterator();
                return moveNextInternal();
            } else { // all done
                rangeToken = null;
                return false;
            }
        }
    }

    private boolean isDone() {
        return state == State.DONE;
    }

    private boolean isClosed() {
        return state == State.CLOSED;
    }

    private RangeToken fetchNextRange(TableRange nextRange) {
        Future<RangeToken> future = executor.submit(
                () -> service.getRange(transactionToken, nextRange));

        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) / 1000 <= REQUEST_TIMEOUT_IN_SEC) {
            try {
                if (cancelFlag.get()) {
                    return null;
                }
                return future.get(REQUEST_POLLING_INTERVAL_IN_SEC, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException  e) {
                close();
                throw Throwables.propagate(e);
            } catch (TimeoutException e) {
                // ignore
            }
        }

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            close();
            throw Throwables.propagate(e);
        }
    }

    private LocalAtlasEnumerator nextIterator() {
        Preconditions.checkNotNull(rangeToken, "You must have a range token before calling nextIterator().");
        List<AtlasRow> rows =
                StreamSupport.stream(rangeToken.getResults().getResults().spliterator(), false)
                        .map(result -> AtlasRows.deserialize(metadata, result))
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        return new LocalAtlasEnumerator(rows.iterator(), cancelFlag);
    }
}
