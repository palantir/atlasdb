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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.calcite.linq4j.Enumerator;

import com.palantir.atlasdb.api.AtlasDbService;
import com.palantir.atlasdb.api.RangeToken;
import com.palantir.atlasdb.api.TableRange;
import com.palantir.atlasdb.api.TransactionToken;
import com.palantir.atlasdb.encoding.PtBytes;

public class PagingAtlasEnumerator implements Enumerator<AtlasRow> {
    private static final int BATCH_SIZE = 500;

    private final AtlasDbService service;
    private final AtlasTableMetadata metadata;
    private final TransactionToken transactionToken;
    private RangeToken rangeToken;
    private LocalAtlasEnumerator curIter;

    public PagingAtlasEnumerator(AtlasDbService service,
            AtlasTableMetadata metadata,
            TransactionToken transactionToken,
            RangeToken rangeToken) {
        this.service = service;
        this.metadata = metadata;
        this.transactionToken = transactionToken;
        this.rangeToken = rangeToken;
        this.curIter = null;
    }

    public static PagingAtlasEnumerator create(AtlasDbService service, AtlasTableMetadata metadata, String tableName) {
        TransactionToken transactionToken = TransactionToken.autoCommit();
        TableRange request = new TableRange(
                tableName,
                PtBytes.EMPTY_BYTE_ARRAY,
                PtBytes.EMPTY_BYTE_ARRAY,
                metadata.namedColumns().stream()
                        .map(AtlasColumnMetdata::getName)
                        .map(String::getBytes)
                        .collect(Collectors.toList()),
                BATCH_SIZE);
        RangeToken rangeToken = service.getRange(transactionToken, request);
        return new PagingAtlasEnumerator(service, metadata, transactionToken, rangeToken);
    }

    @Override
    public AtlasRow current() {
        if (isClosed()) {
            return null;
        }
        return curIter.current();
    }

    @Override
    public boolean moveNext() {
        if (isClosed()) {
            throw new NoSuchElementException("Results iterator is closed.");
        }
        if (hasNext()) {
            curIter.moveNext();
            return true;
        }
        return false;
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        rangeToken = null;
        if (curIter != null) {
            curIter.close();
        }
    }

    private boolean hasNext() {
        if (isClosed()) {
            return false;
        }

        if (curIter == null) {
            curIter = nextIterator();
        }

        if (curIter.hasNext()) {
            return true;
        } else { // page to the next range
            if (rangeToken.hasMoreResults()) {
                rangeToken = service.getRange(transactionToken, rangeToken.getNextRange());
                curIter = nextIterator();
                return hasNext();
            } else { // all done
                rangeToken = null;
                return false;
            }
        }
    }

    private boolean isClosed() {
        return rangeToken == null || (curIter != null && curIter.isClosed());
    }

    private LocalAtlasEnumerator nextIterator() {
        List<AtlasRow> rows =
                StreamSupport.stream(rangeToken.getResults().getResults().spliterator(), false)
                        .map(result -> AtlasRows.deserialize(metadata, result))
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        return new LocalAtlasEnumerator(rows.iterator());
    }
}
