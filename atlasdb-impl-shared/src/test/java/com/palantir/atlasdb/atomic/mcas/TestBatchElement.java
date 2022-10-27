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
import com.palantir.atlasdb.autobatch.DisruptorAutobatcher;
import com.palantir.atlasdb.keyvalue.api.Cell;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

@SuppressWarnings("immutables:subtype")
@org.immutables.value.Value.Immutable
interface TestBatchElement extends BatchElement<CasRequest, Void> {
    @org.immutables.value.Value.Parameter
    @Nullable
    @Override
    CasRequest argument();

    @org.immutables.value.Value.Parameter
    @Override
    DisruptorAutobatcher.DisruptorFuture<Void> result();

    static TestBatchElement of(Cell cell, ByteBuffer expected, ByteBuffer update) {
        return ImmutableTestBatchElement.builder()
                .argument(ImmutableCasRequest.of(cell, expected, update))
                .result(new DisruptorAutobatcher.DisruptorFuture<>("test"))
                .build();
    }
}
