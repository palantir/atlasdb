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

import static com.palantir.logsafe.testing.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.palantir.atlasdb.autobatch.BatchElement;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetException;
import com.palantir.atlasdb.keyvalue.api.MultiCheckAndSetRequest;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.logging.LoggingArgs;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Test;

public final class CasRequestBatchTest {
    private static final byte[] EXPECTED = PtBytes.toBytes(1);
    private static final ByteBuffer WRAPPED_EXPECTED = ByteBuffer.wrap(EXPECTED);

    private static final byte[] UPDATE = PtBytes.toBytes(2889);
    private static final ByteBuffer WRAPPED_UPDATE = ByteBuffer.wrap(UPDATE);
    private static final TableReference TABLE_REFERENCE = TableReference.createFromFullyQualifiedName("i.am_here");
    private static final ByteBuffer ROW_NAME = ByteBuffer.wrap(PtBytes.toBytes("r"));
    private static final Cell CELL = Cell.create(ROW_NAME.array(), PtBytes.toBytes("c"));

    @Test
    public void canCreateMultiCheckAndSetRequest() {
        CasRequestBatch casRequestBatch = getCasRequestBatch(ImmutableList.of(TestBatchElement.of(CELL,
                WRAPPED_EXPECTED,
                WRAPPED_UPDATE)));
        MultiCheckAndSetRequest mcasRequest = casRequestBatch.getMcasRequest();
        assertThat(mcasRequest.tableRef()).isEqualTo(TABLE_REFERENCE);
        assertThat(mcasRequest.rowName()).isEqualTo(ROW_NAME.array());
        assertThat(mcasRequest.expected()).hasSize(1);
        assertThat(mcasRequest.expected().values()).containsOnly(EXPECTED);
        assertThat(mcasRequest.updates()).hasSize(1);
        assertThat(mcasRequest.updates().values()).containsOnly(UPDATE);
    }

    @Test
    public void canSetSuccessToAllRequests() {
        TestBatchElement elem = TestBatchElement.of(
                CELL,
                WRAPPED_EXPECTED,
                WRAPPED_UPDATE);
        CasRequestBatch casRequestBatch = getCasRequestBatch(ImmutableList.of(elem));
        casRequestBatch.setSuccessForAllRequests();
        assertThatCode(() -> elem.result().get()).doesNotThrowAnyException();
        assertThat(casRequestBatch.isBatchServed()).isTrue();
    }

    @Test
    public void canProcessBatchWithException() {
        TestBatchElement elem = TestBatchElement.of(
                CELL,
                WRAPPED_EXPECTED,
                WRAPPED_UPDATE);
        CasRequestBatch casRequestBatch = getCasRequestBatch(ImmutableList.of(elem));

        // the cell already has the actual value of UPDATE
        MultiCheckAndSetException ex  = new MultiCheckAndSetException(LoggingArgs.tableRef(TABLE_REFERENCE),
                ROW_NAME.array(),
                ImmutableMap.of(CELL, EXPECTED),
                ImmutableMap.of(CELL, UPDATE));
        casRequestBatch.processBatchWithException((_u, _v) -> false, ex);
        assertThatCode(() -> elem.result().get()).doesNotThrowAnyException();
        assertThat(casRequestBatch.isBatchServed()).isTrue();
    }

    private CasRequestBatch getCasRequestBatch(List<BatchElement<CasRequest, Void>> casRequests) {
        return new CasRequestBatch(TABLE_REFERENCE, ROW_NAME, casRequests);
    }
}