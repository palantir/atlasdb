/*
 * (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
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

package com.palantir.atlasdb.atomic.mcas;

import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.common.streams.KeyedStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public final class CasRequestBatch {
    private final TableReference tableRef;
    private final ByteBuffer rowName;

    private List<BatchElement<CasRequest, Void>> pendingRequests;

    public CasRequestBatch(
            TableReference tableRef, ByteBuffer rowName, List<BatchElement<CasRequest, Void>> pendingRequests) {
        this.tableRef = tableRef;
        this.rowName = rowName;
        this.pendingRequests = pendingRequests;
    }

    boolean isBatchServed() {
        return pendingRequests.isEmpty();
    }

    public ByteBuffer getRowName() {
        return rowName;
    }

    public List<BatchElement<CasRequest, Void>> getPendingRequests() {
        return pendingRequests;
    }

    public MultiCheckAndSetRequest getMcasRequest() {
        return multiCasRequest(tableRef, rowName.array(), pendingRequests);
    }

    private static MultiCheckAndSetRequest multiCasRequest(
            TableReference tableRef, byte[] rowName, List<BatchElement<CasRequest, Void>> requests) {
        Map<Cell, byte[]> expected = extractValueMap(requests, CasRequest::expected);
        Map<Cell, byte[]> updates = extractValueMap(requests, CasRequest::update);
        return MultiCheckAndSetRequest.multipleCells(tableRef, rowName, expected, updates);
    }

    private static Map<Cell, byte[]> extractValueMap(
            List<BatchElement<CasRequest, Void>> requests, Function<CasRequest, ByteBuffer> valueExtractor) {
        return KeyedStream.of(requests)
                .mapKeys(elem -> elem.argument().cell())
                .map(elem -> valueExtractor.apply(elem.argument()).array())
                .collectToMap();
    }

    // todo(snanda): may need protection
    public void setPendingRequests(List<BatchElement<CasRequest, Void>> pendingRequests) {
        this.pendingRequests = pendingRequests;
    }
}
