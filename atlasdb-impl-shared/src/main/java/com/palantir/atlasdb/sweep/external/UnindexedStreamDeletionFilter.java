/*
 * Copyright 2018 Palantir Technologies, Inc. All rights reserved.
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

package com.palantir.atlasdb.sweep.external;

import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.keyvalue.api.ColumnSelection;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.transaction.api.Transaction;

/**
 * This filter returns the stream IDs of streams which are being completely deleted from a stream's Index table
 * in a given sweep (e.g. because they were unmarked as used).
 *
 * It is a generic implementation of the filter step in the generated deletion task for stream index tables.
 */
public class UnindexedStreamDeletionFilter implements GenericStreamDeletionFilter {
    private final TableReference indexTableRef;
    private final GenericStreamStoreRowDecoder rowDecoder;

    public UnindexedStreamDeletionFilter(
            TableReference indexTableRef,
            GenericStreamStoreRowDecoder rowDecoder) {
        this.indexTableRef = indexTableRef;
        this.rowDecoder = rowDecoder;
    }

    @Override
    public Set<GenericStreamIdentifier> getStreamIdentifiersToDelete(
            Transaction tx,
            Set<GenericStreamIdentifier> identifiers) {
        Set<GenericStreamIdentifier> identifiersInDb = runGetRowsQuery(tx, identifiers)
                .keySet()
                .stream()
                .map(rowDecoder::decodeIndexOrMetadataTableRow)
                .collect(Collectors.toSet());
        return ImmutableSet.copyOf(Sets.difference(identifiers, identifiersInDb));
    }

    private SortedMap<byte[], RowResult<byte[]>> runGetRowsQuery(Transaction tx,
            Set<GenericStreamIdentifier> identifiers) {
        return tx.getRows(indexTableRef,
                identifiers.stream().map(GenericStreamIdentifier::data).collect(Collectors.toSet()),
                ColumnSelection.all());
    }
}
