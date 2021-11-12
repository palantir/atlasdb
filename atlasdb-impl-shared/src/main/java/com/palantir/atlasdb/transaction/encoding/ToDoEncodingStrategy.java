/*
 * (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.transaction.encoding;

import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.pue.PutUnlessExistsValue;

@SuppressWarnings("DoNotCallSuggester") // yolo
public enum ToDoEncodingStrategy implements TimestampEncodingStrategy<PutUnlessExistsValue<Long>> {
    INSTANCE;

    @Override
    public Cell encodeStartTimestampAsCell(long startTimestamp) {
        return TicketsEncodingStrategy.INSTANCE.encodeStartTimestampAsCell(startTimestamp);
    }

    @Override
    public long decodeCellAsStartTimestamp(Cell cell) {
        return TicketsEncodingStrategy.INSTANCE.decodeCellAsStartTimestamp(cell);
    }

    // todo(gmaretic): implement
    @Override
    public byte[] encodeCommitTimestampAsValue(long startTimestamp, PutUnlessExistsValue<Long> commitTimestamp) {
        throw new UnsupportedOperationException();
    }

    // todo(gmaretic): implement
    @Override
    public PutUnlessExistsValue<Long> decodeValueAsCommitTimestamp(long startTimestamp, byte[] value) {
        throw new UnsupportedOperationException();
    }

    // todo(gmaretic): this method is not necessary, but it should make the PUE table more efficient
    public byte[] transformStagingToCommitted(byte[] stagingValue) {
        throw new UnsupportedOperationException();
    }
}
